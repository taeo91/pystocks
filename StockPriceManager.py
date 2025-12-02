import os
import sys
import logging
import FinanceDataReader as fdr
from dotenv import load_dotenv
from mysql.connector import Error
import datetime

from dbaccess import dbaccess

class StockPriceManager:
    """
    주식 가격 정보를 관리하는 클래스
    """
    def __init__(self, db_access_obj):
        """
        dbaccess 객체를 인자로 받아 초기화합니다.
        """
        self.db_access = db_access_obj
        self.connection = self.db_access.connect_to_mysql()

    def create_prices_table(self):
        """
        'prices' 테이블을 생성합니다. 이미 테이블이 존재하면 생성하지 않습니다.
        """
        try:
            cursor = self.connection.cursor()
            query = """
            CREATE TABLE IF NOT EXISTS prices (
                price_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                company_id INT NOT NULL,
                trade_date DATE NOT NULL,
                open_price DECIMAL(15, 2),
                high_price DECIMAL(15, 2),
                low_price DECIMAL(15, 2),
                close_price DECIMAL(15, 2),
                volume BIGINT,
                FOREIGN KEY (company_id) REFERENCES companies(company_id),
                UNIQUE KEY (company_id, trade_date)
            )
            """
            cursor.execute(query)
            self.connection.commit()
            logging.info("Table 'prices' created or already exists.")
            cursor.close()
            return True
        except Error as e:
            logging.error(f"Error creating 'prices' table: {e}")
            return False

    def save_daily_prices(self, start_date='2023-01-01', limit=None):
        """
        DB에 저장된 모든 회사에 대해 일별 주가 데이터를 가져와 저장합니다.
        """
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # DB에서 회사 목록 가져오기
            company_query = "SELECT company_id, stock_code FROM companies"
            if limit:
                company_query += f" LIMIT {limit}"
            cursor.execute(company_query)
            companies_to_fetch = cursor.fetchall()
            logging.info(f"Fetching price data for {len(companies_to_fetch)} companies from {start_date}.")

            for company in companies_to_fetch:
                company_id = company['company_id']
                stock_code = company['stock_code']
                
                # DB에서 해당 종목의 가장 최근 날짜 가져오기
                last_date_query = "SELECT MAX(trade_date) FROM prices WHERE company_id = %s"
                cursor.execute(last_date_query, (company_id,))
                last_date_result = cursor.fetchone()
                
                # 시작 날짜 설정: DB에 데이터가 있으면 마지막 날짜+1일, 없으면 기본 start_date
                fetch_start_date = start_date
                if last_date_result and last_date_result.get('MAX(trade_date)'):
                    fetch_start_date = last_date_result['MAX(trade_date)'] + datetime.timedelta(days=1)
                    fetch_start_date = fetch_start_date.strftime('%Y-%m-%d')

                try:
                    # FinanceDataReader로 가격 데이터 가져오기
                    df = fdr.DataReader(stock_code, start=fetch_start_date)
                    if df.empty:
                        logging.info(f"No new price data to fetch for {stock_code} from {fetch_start_date}.")
                        continue

                    data_to_insert = []
                    for trade_date, row in df.iterrows():
                        data_to_insert.append((
                            company_id,
                            trade_date.strftime('%Y-%m-%d'),
                            row['Open'], row['High'], row['Low'], row['Close'], row['Volume']
                        ))
                    
                    # DB에 저장
                    price_query = """
                    INSERT INTO prices (company_id, trade_date, open_price, high_price, low_price, close_price, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        open_price=VALUES(open_price), high_price=VALUES(high_price), low_price=VALUES(low_price), 
                        close_price=VALUES(close_price), volume=VALUES(volume)
                    """
                    cursor.executemany(price_query, data_to_insert)
                    self.connection.commit()
                    logging.info(f"Successfully saved/updated {cursor.rowcount} price records for {stock_code}.")

                except Exception as e:
                    logging.error(f"Failed to process data for {stock_code}: {e}")

            cursor.close()
        except Error as e:
            logging.error(f"Database error while saving prices: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred in save_daily_prices: {e}")