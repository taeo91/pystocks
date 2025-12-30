import os
import time
import datetime
import logging
from dotenv import load_dotenv

from AppManager import get_db_connection
from StockManager import StockManager
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

        # 1 & 2. StockManager 실행: 종목 정보, 재무 데이터, 주가 정보, 기술적 지표 업데이트
        logging.info("[1/2] 주식 데이터(정보, 재무, 시세, 지표) 통합 업데이트 시작...")
        stock_manager = StockManager(db_access)
        
        if stock_manager.create_tables() and stock_manager.indicator_manager.create_indicators_tables():
            stock_manager.save_stock_info(limit=limit)
            
            start_date_str = os.getenv('PRICE_FETCH_START_DATE')
            stock_manager.save_daily_prices(start_date=start_date_str, limit=limit)
            stock_manager.update_all_indicators(limit=limit)
        logging.info("[1/2] 주식 데이터 통합 업데이트 완료.")

        # 3. ValuationManager 실행: 가치 평가 (단계 번호 조정)
        logging.info("[2/2] 가치 평가 계산 및 Excel 파일 저장 시작...")
        valuation_manager = ValuationManager(db_access)
        valuation_manager.calculate_and_save_valuations(limit=limit)
        logging.info("[2/2] 가치 평가 계산 및 저장 완료.")

        logging.info("="*50)
        logging.info("모든 작업 완료: %s", time.ctime())
        logging.info("="*50)