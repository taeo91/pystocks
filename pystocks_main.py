import os
import time
import logging
import sys
from dotenv import load_dotenv
from dbaccess import dbaccess
from CompanyManager import CompanyManager
from StockPriceManager import StockPriceManager

def setup_logging():

    """로깅 설정"""
    log_dir = './logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file_path = os.getenv('LOG_FILE_PATH', f'{log_dir}/anal_stocks.log')
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8'
    )
    return log_file_path

if __name__ == "__main__":

    # .env 파일에서 환경 변수 로드
    load_dotenv()

    stock_count = os.getenv('STOCK_COUNT')
    print(f"STOCK_COUNT: {stock_count}")

    log_file_path = setup_logging()
    logging.info("Stock analysis script started at %s", time.ctime())
    print(f"Stock analysis script started at {time.ctime()}")

    host = os.getenv('DB_HOST')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    database = os.getenv('DB_NAME')

    if not all([host, user, password, database]):
        logging.error("필수 데이터베이스 환경 변수가 설정되지 않았습니다.")
        print("필수 데이터베이스 환경 변수가 설정되지 않았습니다.")        
        sys.exit(1)

    db_access = None
    try:
        # dbaccess 클래스를 사용하여 데이터베이스에 연결
        db_access = dbaccess(host, user, password, database)
        connection = db_access.connect_to_mysql()

        if not connection:
            logging.error("데이터베이스 연결 실패. 프로그램을 종료합니다.")
            print("데이터베이스 연결 실패. 프로그램을 종료합니다.")
            sys.exit(1)

        # --- 이 곳에 데이터베이스를 사용하는 로직을 추가합니다 ---

        
        # companies 클래스를 사용하여 종목 정보 테이블 생성 및 업데이트
        company_manager = CompanyManager(db_access)
        if company_manager.create_companies_table():
            logging.info("종목 정보 업데이트를 시작합니다.")
            company_manager.save_companies_from_fdr()
        
        # StockPriceManager를 사용하여 가격 정보 테이블 생성 및 업데이트
        price_manager = StockPriceManager(db_access) # 이 라인은 이미 올바르게 되어 있을 수 있습니다.
        if price_manager.create_prices_table():
            logging.info("주식 가격 정보 업데이트를 시작합니다.")

            # 환경 변수에서 시작 날짜를 가져오거나 기본값 사용
            start_date = os.getenv('PRICE_FETCH_START_DATE', '2023-01-01')
            price_manager.save_daily_prices(start_date=start_date)

    finally:
        if db_access:
            db_access.close_connection()
    
     