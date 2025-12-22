import os
import time
import datetime
import logging
from dotenv import load_dotenv

from AppManager import get_db_connection
from CompanyManager import CompanyManager
from StockPriceManager import StockPriceManager
from ValuationManager import ValuationManager

if __name__ == "__main__":
    # .env 파일 로드
    load_dotenv()
    
    # 로깅 설정
    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(log_dir, f"pystocks_{datetime.date.today()}.log")),
            logging.StreamHandler()
        ]
    )

    # AppManager를 사용하여 데이터베이스 연결
    with get_db_connection() as db_access:
        logging.info("="*50)
        logging.info("주식 데이터 분석 및 가치 평가 스크립트 시작: %s", time.ctime())
        logging.info("="*50)

        stock_count_str = os.getenv('STOCK_COUNT')
        limit = int(stock_count_str) if stock_count_str else None

        # 1. CompanyManager 실행: 종목 정보 및 재무 데이터 업데이트
        logging.info("[1/3] 종목 정보 및 재무 데이터 업데이트 시작...")
        company_manager = CompanyManager(db_access)
        if company_manager.create_companies_table() and company_manager.create_daily_financials_table():
            company_manager.save_companies_from_fdr(limit=limit)
        logging.info("[1/3] 종목 정보 및 재무 데이터 업데이트 완료.")

        # 2. StockPriceManager 실행: 주가 정보 및 기술적 지표 업데이트
        logging.info("[2/3] 주가 정보 및 기술적 지표 업데이트 시작...")
        price_manager = StockPriceManager(db_access)
        if price_manager.create_prices_table() and price_manager.indicator_manager.create_indicators_tables():
            start_date_str = os.getenv('PRICE_FETCH_START_DATE')
            price_manager.save_daily_prices(start_date=start_date_str, limit=limit)
            price_manager.update_all_indicators(limit=limit)
        logging.info("[2/3] 주가 정보 및 기술적 지표 업데이트 완료.")

        # 3. ValuationManager 실행: 가치 평가
        logging.info("[3/3] 가치 평가 계산 및 Excel 파일 저장 시작...")
        valuation_manager = ValuationManager(db_access)
        valuation_manager.calculate_and_save_valuations(limit=limit)
        logging.info("[3/3] 가치 평가 계산 및 저장 완료.")

        logging.info("="*50)
        logging.info("모든 작업 완료: %s", time.ctime())
        logging.info("="*50)