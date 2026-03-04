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

        # 1. 데이터베이스 테이블 생성 (순서 중요)
        logging.info("[1/6] 데이터베이스 테이블 생성 시작...")
        etf_manager.create_etf_info_table()
        if not stock_manager.create_tables():
            logging.error("주식 관련 테이블 생성에 실패하여 스크립트를 중단합니다.")
            exit()
        valuation_manager.create_valuation_table()
        logging.info("[1/6] 데이터베이스 테이블 생성 완료.")

        # 2. 주식 기본 정보 및 재무 데이터 저장 (KOSPI, KOSDAQ)
        logging.info("[2/6] 주식 기본 정보 및 재무 데이터 업데이트 시작...")
        stock_manager.save_stock_info(limit=limit)
        logging.info("[2/6] 주식 기본 정보 및 재무 데이터 업데이트 완료.")

        # 3. 포트폴리오 종목 가격 정보 업데이트 (이 과정에서 ETF가 companies 테이블에 추가됨)
        logging.info("[3/6] 포트폴리오 종목 가격 정보 업데이트 시작...")
        start_date_str = os.getenv('PRICE_FETCH_START_DATE')
        tickers = portfolio_manager.get_tickers_from_excel(portfolio_file_path)
        if tickers:
            portfolio_manager.fetch_and_save_prices(tickers, market_type='etf', start_date=start_date_str)
            portfolio_manager.fetch_and_save_prices(tickers, market_type='stock', start_date=start_date_str)
        logging.info("[3/6] 포트폴리오 종목 가격 정보 업데이트 완료.")

        # 4. ETF 상세 정보 및 주식 리스크 지표 업데이트
        logging.info("[4/6] ETF 상세 정보 및 주식 리스크 지표 업데이트 시작...")
        etf_manager.update_etf_names_from_naver()
        etf_manager.update_etf_details()
        stock_manager.update_risk_metrics(limit=limit)
        logging.info("[4/6] ETF 상세 정보 및 주식 리스크 지표 업데이트 완료.")

        # 5. ValuationManager 실행: 가치 평가
        logging.info("[5/6] 가치 평가 계산 및 Excel 파일 저장 시작...")
        valuation_manager.calculate_and_save_valuations(limit=limit)
        logging.info("[5/6] 가치 평가 계산 및 저장 완료.")

        # 6. 포트폴리오 Excel 파일에 현재가 업데이트
        logging.info("[6/6] 포트폴리오 Excel 파일 업데이트 시작...")
        portfolio_manager.update_portfolio_excel_with_prices(portfolio_file_path)
        logging.info("[6/6] 포트폴리오 Excel 파일 업데이트 완료.")
        
        logging.info("="*50)
        logging.info("모든 작업 완료: %s", time.ctime())
        logging.info("="*50)