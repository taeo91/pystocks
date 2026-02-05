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