import os
import sys
import logging
import time
from contextlib import contextmanager
from dotenv import load_dotenv
from DBAccessManager import DBAccessManager

def setup_logging():
    """공통 로깅 설정"""
    log_dir = './logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file_path = os.getenv('LOG_FILE_PATH', f'{log_dir}/pystocks.log')
    
    # 루트 로거의 핸들러를 초기화하여 중복 로깅 방지
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        
    logging.basicConfig(
        handlers=[
            logging.FileHandler(log_file_path, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ],
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def _normalize_env_path(value):
    if not value:
        return None
    normalized = str(value).strip()
    if (normalized.startswith('"') and normalized.endswith('"')) or (normalized.startswith("'") and normalized.endswith("'")):
        normalized = normalized[1:-1].strip()
    return normalized


def get_portfolio_excel_path(default='reports/portfolio_r16.xlsx'):
    load_dotenv()
    path = _normalize_env_path(os.getenv('PORTFOLIO_EXCEL_FILE', default))
    if not path:
        logging.error('PORTFOLIO_EXCEL_FILE environment variable not set.')
        return None
    if not os.path.isabs(path):
        path = os.path.join(os.path.dirname(__file__), path)
    if not os.path.exists(path):
        logging.error(f'포트폴리오 파일이 존재하지 않습니다: {path}')
        return None
    return path

@contextmanager
def get_db_connection():
    """데이터베이스 연결을 위한 컨텍스트 매니저"""
    load_dotenv()
    setup_logging()

    host = os.getenv('DB_HOST')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    database = os.getenv('DB_NAME')

    if not all([host, user, password, database]):
        logging.error("필수 데이터베이스 환경 변수가 설정되지 않았습니다.")
        sys.exit(1)

    db_access = None
    try:
        db_access = DBAccessManager(host, user, password, database)
        connection = db_access.connect_to_mysql()

        if not connection:
            logging.error("데이터베이스 연결 실패. 프로그램을 종료합니다.")
            sys.exit(1)
        
        logging.info("데이터베이스 연결 성공.")
        yield db_access

    except Exception as e:
        logging.error(f"An error occurred during DB setup: {e}")
        raise
    finally:
        if db_access:
            db_access.close_connection()
            logging.info("데이터베이스 연결이 종료되었습니다.")