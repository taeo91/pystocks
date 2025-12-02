import os
import time
import logging

from AppManager import get_db_connection
from StockPriceManager import StockPriceManager

if __name__ == "__main__":
    # AppManager를 사용하여 데이터베이스 연결 및 로깅 설정
    with get_db_connection() as db_access:
        logging.info("주가 분석 스크립트 시작: %s", time.ctime())
        
        # StockPriceManager를 사용하여 가격 정보 테이블 생성 및 업데이트
        price_manager = StockPriceManager(db_access)
        if price_manager.create_prices_table():
            logging.info("주식 가격 정보 업데이트를 시작합니다.")

            # 환경 변수에서 시작 날짜를 가져오거나 기본값 사용
            start_date = os.getenv('PRICE_FETCH_START_DATE', '2023-01-01')
            stock_count = os.getenv('STOCK_COUNT')
            limit = int(stock_count) if stock_count else None
            price_manager.save_daily_prices(start_date=start_date, limit=limit)
        
        logging.info("주가 분석 스크립트 종료: %s", time.ctime())