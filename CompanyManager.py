import os
import sys
import logging
import time
import FinanceDataReader as fdr
import pandas as pd
from AppManager import get_db_connection

class CompanyManager:
    """
    주식 종목 정보를 관리하는 클래스
    """
    def __init__(self, db_access):
        """
        CompanyManager 생성자

        Args:
            db_access (dbaccess): 데이터베이스 접근을 위한 dbaccess 객체
        """
        self.db_access = db_access    
        
    def create_companies_table(self):
        """'companies' 테이블을 생성하는 메서드"""
        try:
            query = """
            CREATE TABLE IF NOT EXISTS companies (
                id INT AUTO_INCREMENT PRIMARY KEY,
                code VARCHAR(20) NOT NULL UNIQUE COMMENT '종목코드',
                name VARCHAR(255) NOT NULL COMMENT '종목명',
                market VARCHAR(50) COMMENT '시장',
                sector VARCHAR(255) COMMENT '섹터',
                industry VARCHAR(255) COMMENT '산업',
                listing_date DATE COMMENT '상장일',
                settle_month VARCHAR(50) COMMENT '결산월',
                representative VARCHAR(255) COMMENT '대표',
                homepage VARCHAR(255) COMMENT '홈페이지',
                region VARCHAR(255) COMMENT '지역'
            ) COMMENT '종목 정보';
            """
            self.db_access.execute_query(query)
            logging.info("Table 'companies' created or already exists.")
            print("Table 'companies' created or already exists.")
            return True 
        except Exception as e:
            logging.error(f"Error creating 'companies' table: {e}")
            print(f"Error creating 'companies' table: {e}")
            return False

    def save_companies_from_fdr(self):
        """FinanceDataReader를 사용하여 KRX 전체 종목 정보를 가져와 DB에 저장하는 메서드"""
        try:
            logging.info("Fetching company list from KRX...")
            print("Fetching company list from KRX...")
            # KRX 전체 종목 리스트 가져오기
            stocks = fdr.StockListing('KRX') # KRX는 코스피, 코스닥, 코넥스, ETF, ETN 등을 모두 포함합니다.

            # 'ListingDate' 열이 있으면 날짜 형식을 처리하고, 없으면 열을 생성하여 None으로 채웁니다.
            if 'ListingDate' in stocks.columns:
                stocks['ListingDate'] = pd.to_datetime(stocks['ListingDate'], errors='coerce')
                stocks['ListingDate'] = stocks['ListingDate'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)
            else:
                stocks['ListingDate'] = None

            # DB에 이미 저장된 종목 코드들을 가져옵니다.
            cursor = self.db_access.connection.cursor()
            cursor.execute("SELECT code FROM companies")
            existing_codes = {row[0] for row in cursor.fetchall()}
            cursor.close()
            
            logging.info(f"DB에 존재하는 종목 수: {len(existing_codes)}")

            # DB에 없는 새로운 종목만 필터링합니다.
            new_stocks = stocks[~stocks['Code'].isin(existing_codes)]

            if new_stocks.empty:
                logging.info("새로 추가할 종목이 없습니다.")
                print("새로 추가할 종목이 없습니다.")
                return

            logging.info(f"총 {len(new_stocks)}개의 새로운 종목을 데이터베이스에 저장합니다.")
            print(f"총 {len(new_stocks)}개의 새로운 종목을 데이터베이스에 저장합니다.")

            for _, row in new_stocks.iterrows():
                query = """
                INSERT INTO companies (code, name, market, sector, industry, listing_date, settle_month, representative, homepage, region)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                params = (
                    row.get('Code'), row.get('Name'), row.get('Market'), row.get('Sector'), row.get('Industry'),
                    row.get('ListingDate'),
                    row.get('SettleMonth'), row.get('Representative'), row.get('HomePage'), row.get('Region')
                )
                self.db_access.execute_query(query, params)
            
            logging.info(f"성공적으로 {len(new_stocks)}개의 신규 종목 정보를 저장했습니다.")
            print(f"성공적으로 {len(new_stocks)}개의 신규 종목 정보를 저장했습니다.")

        except Exception as e:
            logging.error(f"Error saving companies from fdr: {e}")
            print(f"Error saving companies from fdr: {e}")

if __name__ == "__main__":
    # AppManager를 사용하여 데이터베이스 연결 및 로깅 설정.
    # 이 컨텍스트 블록 내에서 db_access 객체를 사용할 수 있습니다.
    with get_db_connection() as db_access: # get_db_connection은 AppManager.py에 정의되어 있습니다.
        logging.info("CompanyManager 스크립트 시작: %s", time.ctime())

        company_manager = CompanyManager(db_access)
        if company_manager.create_companies_table():
            logging.info("종목 정보 업데이트를 시작합니다.")
            company_manager.save_companies_from_fdr()

        logging.info("CompanyManager 스크립트 종료: %s", time.ctime())