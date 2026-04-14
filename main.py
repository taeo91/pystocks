import os
import time
import datetime
import logging
from dotenv import load_dotenv

from AppManager import get_db_connection, get_portfolio_excel_path
from StockManager import StockManager
from ValuationManager import ValuationManager
from ETFManager import ETFManager
from PortfolioManager import PortfolioManager

if __name__ == "__main__":
    # .env 파일 로드
    load_dotenv()
    
    # .env 파일에서 포트폴리오 파일 경로를 로드합니다.
    portfolio_file_path = get_portfolio_excel_path()
    
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
        logging.info("[1/5] 데이터베이스 테이블 생성 시작...")
        etf_manager.create_etf_info_table()
        if not stock_manager.create_tables():
            logging.error("주식 관련 테이블 생성에 실패하여 스크립트를 중단합니다.")
            exit()
        valuation_manager.create_valuation_table()
        logging.info("[1/5] 데이터베이스 테이블 생성 완료.")

        start_date_str = os.getenv('PRICE_FETCH_START_DATE')

        # 2. 주식 정보 및 가격 처리
        logging.info("[2/5] 주식 정보 및 가격 데이터 업데이트 시작...")
        stock_manager.save_stock_info(limit=limit)
        stock_manager.save_daily_prices(start_date=start_date_str, limit=limit)
        stock_manager.update_risk_metrics(limit=limit)
        logging.info("[2/5] 주식 정보 및 가격 데이터 업데이트 완료.")

        # 3. 주식 가치 평가
        logging.info("[3/5] 가치 평가 계산 및 Excel 파일 저장 시작...")
        valuation_manager.calculate_and_save_valuations(limit=limit)
        logging.info("[3/5] 가치 평가 계산 및 저장 완료.")

        # 4. ETF 처리
        logging.info("[4/5] ETF 정보 및 가격 데이터 업데이트 시작...")
        etf_manager.save_etf_info()
        etf_manager.save_daily_prices(start_date=start_date_str, limit=limit)
        logging.info("[4/5] ETF 정보 및 가격 데이터 업데이트 완료.")

        # 5. 포트폴리오 처리
        if portfolio_file_path:
            logging.info(f"[5/5] 포트폴리오 업데이트 시작... (파일: {portfolio_file_path})")
            tickers = portfolio_manager.get_tickers_from_excel(portfolio_file_path)
            logging.info(f"모든 포트폴리오 시트에서 총 {len(tickers)}개의 고유 티커를 수집했습니다. 가격 정보를 가져옵니다.")
            if tickers:
                portfolio_manager.fetch_and_save_prices(tickers, market_type='etf', start_date=start_date_str)
                portfolio_manager.fetch_and_save_prices(tickers, market_type='stock', start_date=start_date_str)
            portfolio_manager.update_portfolio_excel_with_prices(portfolio_file_path)
            logging.info("[5/5] 포트폴리오 업데이트 완료.")
        else:
            logging.error("[5/5] 포트폴리오 파일이 없어서 업데이트를 건너뜁니다.")
        
        logging.info("="*50)
        logging.info("모든 작업 완료: %s", time.ctime())
        logging.info("="*50)