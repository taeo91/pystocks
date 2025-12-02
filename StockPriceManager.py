import os
import sys
import logging
import time
import datetime
import FinanceDataReader as fdr
from dotenv import load_dotenv
from AppManager import get_db_connection

class StockPriceManager:
    """
    주식 가격 정보를 관리하는 클래스
    """
    def __init__(self, db_access_obj):
        """dbaccess 객체를 인자로 받아 초기화합니다."""
        self.db_access = db_access_obj    

    def create_prices_table(self):
        """'prices' 테이블을 생성합니다. 이미 테이블이 존재하면 생성하지 않습니다."""
        try:
            query = """
            CREATE TABLE IF NOT EXISTS prices (
                price_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                company_id INT NOT NULL,
                trade_date DATE NOT NULL,
                open_price DECIMAL(15, 2) NULL,
                high_price DECIMAL(15, 2) NULL,
                low_price DECIMAL(15, 2) NULL,
                close_price DECIMAL(15, 2) NULL,
                volume BIGINT,
                FOREIGN KEY (company_id) REFERENCES companies(id),
                UNIQUE KEY (company_id, trade_date)
            )
            """
            self.db_access.execute_query(query)
            logging.info("Table 'prices' created or already exists.")
            return True
        except Exception as e:
            logging.error(f"Error creating 'prices' table: {e}")
            return False

    def save_daily_prices(self, start_date=None, limit=None):
        """DB에 저장된 모든 회사에 대해 일별 주가 데이터를 가져와 저장합니다."""
        try:
            cursor = self.db_access.connection.cursor(dictionary=True)
            
            # DB에서 회사 목록 가져오기
            company_query = "SELECT id, code FROM companies"
            if limit:
                company_query += f" LIMIT {limit}"
            cursor.execute(company_query)
            companies_to_fetch = cursor.fetchall()

            # start_date가 None이면 1년 전 날짜로 설정
            if start_date is None:
                start_date = (datetime.date.today() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')

            logging.info(f"Fetching price data for {len(companies_to_fetch)} companies. Default start date: {start_date}.")

            for company in companies_to_fetch:
                company_id = company['id']
                stock_code = company['code']
                
                # DB에서 해당 종목의 가장 최근 날짜 가져오기
                last_date_query = "SELECT MAX(trade_date) FROM prices WHERE company_id = %s"
                cursor.execute(last_date_query, (company_id,))
                last_date_result = cursor.fetchone()
                
                fetch_start_date = start_date
                if last_date_result and last_date_result.get('MAX(trade_date)'):
                    fetch_start_date = last_date_result['MAX(trade_date)'] + datetime.timedelta(days=1)
                    fetch_start_date = fetch_start_date.strftime('%Y-%m-%d')

                try:
                    df = fdr.DataReader(stock_code, start=fetch_start_date)
                    if df.empty:
                        logging.info(f"No new price data to fetch for {stock_code} from {fetch_start_date}.")
                        continue

                    data_to_insert = [
                        (company_id, trade_date.strftime('%Y-%m-%d'), row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])
                        for trade_date, row in df.iterrows()
                    ]
                    
                    if data_to_insert:
                        price_query = """
                        INSERT INTO prices (company_id, trade_date, open_price, high_price, low_price, close_price, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                            open_price=VALUES(open_price), high_price=VALUES(high_price), low_price=VALUES(low_price), 
                            close_price=VALUES(close_price), volume=VALUES(volume)
                        """
                        insert_cursor = self.db_access.connection.cursor()
                        insert_cursor.executemany(price_query, data_to_insert)
                        self.db_access.connection.commit()
                        logging.info(f"Successfully saved/updated {insert_cursor.rowcount} price records for {stock_code}.")
                        insert_cursor.close()

                except Exception as e:
                    # 404 오류가 발생하면 해당 종목이 더 이상 유효하지 않은 것으로 간주합니다.
                    if '404' in str(e):
                        logging.warning(f"'{stock_code}' 종목 데이터를 찾을 수 없어(404 Not Found) DB에서 삭제를 시도합니다.")
                        try:
                            # 외래 키 제약 조건을 위해 prices 테이블에서 먼저 삭제
                            delete_prices_query = "DELETE FROM prices WHERE company_id = %s"
                            self.db_access.execute_query(delete_prices_query, (company_id,))
                            
                            # companies 테이블에서 최종 삭제
                            delete_company_query = "DELETE FROM companies WHERE id = %s"
                            self.db_access.execute_query(delete_company_query, (company_id,))
                            logging.info(f"'{stock_code}' 종목이 DB에서 성공적으로 삭제되었습니다.")
                        except Exception as delete_e:
                            logging.error(f"'{stock_code}' 종목 DB 삭제 중 오류 발생: {delete_e}")
                    else:
                        logging.error(f"'{stock_code}' 종목 데이터 처리 중 오류 발생: {e}")

            cursor.close()
        except Exception as e:
            logging.error(f"An unexpected error occurred in save_daily_prices: {e}")

if __name__ == "__main__":
    with get_db_connection() as db_access:
        logging.info("StockPriceManager 스크립트 시작: %s", time.ctime())
        
        price_manager = StockPriceManager(db_access)
        if price_manager.create_prices_table():
            logging.info("주식 가격 정보 업데이트를 시작합니다.")

            one_year_ago = datetime.date.today() - datetime.timedelta(days=365)
            start_date = os.getenv('PRICE_FETCH_START_DATE', one_year_ago.strftime('%Y-%m-%d'))

            stock_count = os.getenv('STOCK_COUNT')
            limit = int(stock_count) if stock_count else None
            price_manager.save_daily_prices(start_date=start_date, limit=limit)
        
        logging.info("StockPriceManager 스크립트 종료: %s", time.ctime())
