import os
import sys
import logging
import FinanceDataReader as fdr
from dotenv import load_dotenv
from mysql.connector import Error

from dbaccess import dbaccess

class CompanyManager:
    """
    주식 종목 정보를 관리하는 클래스
    """
    def __init__(self, db_access_obj):
        """
        dbaccess 객체를 인자로 받아 초기화합니다.
        """
        self.db_access = db_access_obj
        self.connection = self.db_access.connect_to_mysql()

    def create_companies_table(self):
        """
        'companies' 테이블을 생성합니다. 이미 테이블이 존재하면 생성하지 않습니다.
        """
        try:
            cursor = self.connection.cursor()
            query = """
            CREATE TABLE IF NOT EXISTS companies (
                company_id INT AUTO_INCREMENT PRIMARY KEY,
                company_name VARCHAR(100) NOT NULL,
                stock_code VARCHAR(20) NOT NULL UNIQUE,
                market VARCHAR(50)
            )
            """
            cursor.execute(query)
            self.connection.commit()
            logging.info("Table 'companies' created or already exists.")
            cursor.close()
        except Error as e:
            logging.error(f"Error creating 'companies' table: {e}")
            return False
        return True

    def save_companies_from_fdr(self):
        """
        FinanceDataReader를 통해 KRX 전체 종목 리스트를 가져와 DB에 저장합니다.
        stock_code가 중복될 경우, company_name과 market을 업데이트합니다.
        """
        try:
            logging.info("Fetching stock listings from KRX...")
            krx_stocks = fdr.StockListing('KRX')
            logging.info(f"Fetched {len(krx_stocks)} stock items.")

            cursor = self.connection.cursor()
            query = """
            INSERT INTO companies (company_name, stock_code, market)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE company_name = VALUES(company_name), market = VALUES(market)
            """
            
            data_to_insert = []
            for index, row in krx_stocks.iterrows():
                data_to_insert.append((row['Name'], row['Code'], row['Market']))

            cursor.executemany(query, data_to_insert)
            self.connection.commit()
            logging.info(f"Successfully saved/updated {cursor.rowcount} companies in the database.")
            cursor.close()

        except Error as e:
            logging.error(f"Error saving companies to database: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")