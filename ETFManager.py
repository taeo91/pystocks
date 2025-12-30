import logging
import os
import time
import datetime
import FinanceDataReader as fdr
import pandas as pd
from AppManager import get_db_connection

class ETFManager:
    """
    ETF 종목 정보와 시세를 관리하는 클래스
    """
    def __init__(self, db_access):
        """
        ETFManager 생성자

        Args:
            db_access (dbaccess): 데이터베이스 접근을 위한 dbaccess 객체
        """
        self.db_access = db_access

    def create_etf_tables(self):
        """ETF 정보와 시세 정보를 저장할 테이블을 생성하는 메서드"""
        try:
            # ETF 기본 정보 테이블
            query_etf_info = """
            CREATE TABLE IF NOT EXISTS etfs (
                code VARCHAR(20) PRIMARY KEY COMMENT 'ETF 종목코드',
                name VARCHAR(255) NOT NULL COMMENT 'ETF 종목명',
                net_assets BIGINT COMMENT '순자산총액(억원)'
            ) COMMENT 'ETF 기본 정보';
            """
            self.db_access.execute_query(query_etf_info)

            # ETF 일별 시세 테이블
            query_etf_prices = """
            CREATE TABLE IF NOT EXISTS etf_prices (
                id INT AUTO_INCREMENT PRIMARY KEY,
                code VARCHAR(20) NOT NULL COMMENT 'ETF 종목코드',
                date DATE NOT NULL COMMENT '기준일자',
                open INT COMMENT '시가',
                high INT COMMENT '고가',
                low INT COMMENT '저가',
                close INT COMMENT '종가',
                volume BIGINT COMMENT '거래량',
                change_rate DECIMAL(10, 2) COMMENT '등락률(%)',
                FOREIGN KEY (code) REFERENCES etfs (code) ON DELETE CASCADE,
                UNIQUE KEY (code, date)
            ) COMMENT 'ETF 일별 시세';
            """
            self.db_access.execute_query(query_etf_prices)
            logging.info("Tables 'etfs' and 'etf_prices' created or already exist.")
            return True
        except Exception as e:
            logging.error(f"Error creating ETF tables: {e}")
            return False

    def save_etf_info(self, limit=None):
        """FinanceDataReader에서 ETF 목록을 가져와 'etfs' 테이블에 저장"""
        try:
            logging.info("ETF 목록을 업데이트합니다...")
            df_etf = self.get_etf_listings()
            if df_etf.empty:
                return

            # 시가총액(MarCap) 또는 순자산(NetAssets) 컬럼 확인
            # FDR 버전에 따라 컬럼명이 다를 수 있으므로 확인 후 사용 (MarCap 우선)
            asset_col = 'MarCap' if 'MarCap' in df_etf.columns else ('NetAssets' if 'NetAssets' in df_etf.columns else None)

            # 자산규모 순으로 정렬 (내림차순)
            if asset_col:
                df_etf = df_etf.sort_values(by=asset_col, ascending=False)

            # 코드가 숫자가 아닌 경우(예: 0007F0) FDR에서 Yahoo로 조회되어 오류가 발생할 수 있으므로 미리 제외
            if 'Symbol' in df_etf.columns:
                df_etf = df_etf[df_etf['Symbol'].apply(lambda x: str(x).isdigit())]

            # 설정된 개수만큼만 자르기
            if limit:
                df_etf = df_etf.head(limit)

            etfs_data = []
            for _, row in df_etf.iterrows():
                # 시가총액/순자산 가져오기
                net_assets = None
                if asset_col:
                    val = row.get(asset_col)
                    if pd.notna(val):
                        net_assets = val
                
                etfs_data.append((row['Symbol'], row['Name'], net_assets))

            if etfs_data:
                query = """
                INSERT INTO etfs (code, name, net_assets) VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    net_assets = VALUES(net_assets);
                """
                self.db_access.execute_many_query(query, etfs_data)
                logging.info(f"ETF {len(etfs_data)}개 종목 정보 저장 완료.")
        except Exception as e:
            logging.error(f"ETF 정보 저장 실패: {e}")

    def save_etf_daily_prices(self, code, start_date=None, end_date=None):
        """특정 ETF의 일별 시세를 'etf_prices' 테이블에 저장"""
        try:
            df = self.get_etf_price_data(code, start_date, end_date)
            if df is None or df.empty:
                return

            prices_data = []
            for date_idx, row in df.iterrows():
                change_rate = row.get('Change')
                if pd.notna(change_rate):
                    change_rate = change_rate * 100 # 0.01 -> 1.00%

                prices_data.append((
                    code, date_idx.date(),
                    int(row['Open']), int(row['High']), int(row['Low']), int(row['Close']),
                    int(row['Volume']), change_rate
                ))

            if prices_data:
                query = """
                INSERT INTO etf_prices (code, date, open, high, low, close, volume, change_rate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    open = VALUES(open), high = VALUES(high), low = VALUES(low),
                    close = VALUES(close), volume = VALUES(volume), change_rate = VALUES(change_rate);
                """
                self.db_access.execute_many_query(query, prices_data)
        except Exception as e:
            # FDR이 비표준 코드(예: 0007F0)를 Yahoo Finance로 조회하다 404가 발생한 경우 처리
            if '404' in str(e) and 'yahoo' in str(e).lower():
                logging.warning(f"ETF({code}) 시세 수집 실패(Yahoo 404). 유효하지 않은 코드이므로 DB에서 삭제합니다.")
                try:
                    self.db_access.execute_query("DELETE FROM etfs WHERE code = %s", (code,))
                except Exception as del_e:
                    logging.error(f"ETF({code}) 삭제 실패: {del_e}")
            else:
                logging.error(f"ETF({code}) 시세 저장 실패: {e}")

    def save_all_etf_daily_prices(self):
        """etfs 테이블에 있는 모든 종목의 시세 정보를 저장 (최신 데이터만 추가)"""
        try:
            # ETF 목록 가져오기
            query = "SELECT code FROM etfs"
            etfs = self.db_access.fetch_all(query)

            if not etfs:
                logging.info("저장된 ETF 종목이 없습니다.")
                return

            # 기본 시작일 (1년 전)
            default_start_date = (datetime.date.today() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
            
            logging.info(f"총 {len(etfs)}개 ETF의 시세 정보를 업데이트합니다.")

            for row in etfs:
                code = row[0]
                
                # DB에서 해당 ETF의 가장 최근 날짜 가져오기
                query_last_date = "SELECT MAX(date) FROM etf_prices WHERE code = %s"
                last_date_result = self.db_access.fetch_one(query_last_date, (code,))
                
                start_date = default_start_date
                if last_date_result and last_date_result[0]:
                    start_date = (last_date_result[0] + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                
                # 미래 날짜인 경우 스킵
                if start_date > datetime.date.today().strftime('%Y-%m-%d'):
                    continue

                self.save_etf_daily_prices(code, start_date=start_date)
                
        except Exception as e:
            logging.error(f"ETF 전체 시세 저장 중 오류 발생: {e}")

    def get_etf_listings(self):
        """
        FinanceDataReader를 사용하여 국내 상장 ETF 목록을 가져오는 메서드
        반환되는 DataFrame 컬럼: Symbol, Name, NetAssets, Eer, Amo, Gross, etc.
        """
        try:
            logging.info("KRX에서 ETF 종목 목록을 가져옵니다...")
            # 'ETF/KR'을 사용하여 한국 ETF 목록을 가져옵니다.
            df_etf = fdr.StockListing('ETF/KR')
            return df_etf
        except Exception as e:
            logging.error(f"ETF 목록을 가져오는 중 오류 발생: {e}")
            return pd.DataFrame()

    def get_etf_price_data(self, code, start_date=None, end_date=None):
        """특정 ETF의 가격 정보를 가져오는 메서드"""
        # start_date가 없으면 기본적으로 최근 데이터를 가져오거나 fdr 기본 동작을 따름
        return fdr.DataReader(code, start_date, end_date)

if __name__ == "__main__":
    with get_db_connection() as db_access:
        logging.basicConfig(level=logging.INFO)
        logging.info("ETFManager 스크립트 시작: %s", time.ctime())
        
        etf_manager = ETFManager(db_access)
        if etf_manager.create_etf_tables():
            # .env 파일에서 STOCK_COUNT 가져오기
            stock_count_str = os.getenv('STOCK_COUNT')
            limit = int(stock_count_str) if stock_count_str else None
            etf_manager.save_etf_info(limit=limit)
            etf_manager.save_all_etf_daily_prices()
            
        logging.info("ETFManager 스크립트 종료: %s", time.ctime())