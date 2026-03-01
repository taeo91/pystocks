import os
import time
import datetime
import logging
from dotenv import load_dotenv

from AppManager import get_db_connection
from StockManager import StockManager
from ValuationManager import ValuationManager
from ETFManager import ETFManager
from PortfolioManager import PortfolioManager

if __name__ == "__main__":
    # .env 파일 로드
    load_dotenv()
    
    # .env 파일에서 포트폴리오 파일 경로를 로드합니다. 없으면 기본값을 사용합니다.
    portfolio_file_path = os.getenv('PORTFOLIO_EXCEL_FILE', 'reports/portfolio_daily_r9_py.xlsx')
    
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

        # Manager 인스턴스화
        etf_manager = ETFManager(db_access)
        portfolio_manager = PortfolioManager(db_access, etf_manager)
        stock_manager = StockManager(db_access)
        valuation_manager = ValuationManager(db_access)

        # 1. ETF 데이터 업데이트
        logging.info("[1/4] ETF 데이터 업데이트 시작...")
        etf_manager.create_etf_info_table()
        tickers = portfolio_manager.get_tickers_from_excel(portfolio_file_path)
        if tickers:
            portfolio_manager.fetch_and_save_etf_prices(tickers)
        etf_manager.update_etf_names_from_naver()
        etf_manager.update_etf_details()
        logging.info("[1/4] ETF 데이터 업데이트 완료.")

        # 2. StockManager 실행: 종목 정보, 재무 데이터, 주가 정보, 기술적 지표 업데이트
        logging.info("[2/4] 주식 데이터(정보, 재무, 시세) 통합 업데이트 시작...")
        if stock_manager.create_tables():
            stock_manager.save_stock_info(limit=limit)
            
            start_date_str = os.getenv('PRICE_FETCH_START_DATE')
            stock_manager.save_daily_prices(start_date=start_date_str, limit=limit)
            stock_manager.update_risk_metrics(limit=limit)
        logging.info("[2/4] 주식 데이터 통합 업데이트 완료.")

        # 3. ValuationManager 실행: 가치 평가
        logging.info("[3/4] 가치 평가 계산 및 Excel 파일 저장 시작...")
        valuation_manager.calculate_and_save_valuations(limit=limit)
        logging.info("[3/4] 가치 평가 계산 및 저장 완료.")

        # 4. 포트폴리오 Excel 파일에 현재가 업데이트
        logging.info("[4/4] 포트폴리오 Excel 파일 업데이트 시작...")
        portfolio_manager.update_portfolio_excel_with_prices(portfolio_file_path)
        logging.info("[4/4] 포트폴리오 Excel 파일 업데이트 완료.")
        
        logging.info("="*50)
        logging.info("모든 작업 완료: %s", time.ctime())
        logging.info("="*50)