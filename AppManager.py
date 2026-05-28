import os
import sys
import glob
import shutil
import logging
import time
import datetime
from contextlib import contextmanager
from dotenv import load_dotenv
from DBAccessManager import DBAccessManager

def setup_logging():
    """공통 로깅 설정"""
    import datetime
    log_dir = './logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file_path = os.path.join(log_dir, f"pystocks_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log")
    
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


def get_or_create_today_portfolio():
    """오늘 날짜 포트폴리오 파일 경로를 반환합니다.

    파일이 없으면 reports/ 디렉터리에서 가장 최근 portfolio_YYMMDD.xlsx를
    오늘 날짜로 복사해 생성합니다. 날짜 파일이 전혀 없으면 r16 파일을 원본으로 씁니다.
    """
    reports_dir = os.path.join(os.path.dirname(__file__), 'reports')
    os.makedirs(reports_dir, exist_ok=True)

    today_str = datetime.date.today().strftime('%y%m%d')   # e.g. '260528'
    today_path = os.path.join(reports_dir, f'portfolio_{today_str}.xlsx')

    if os.path.exists(today_path):
        logging.info(f"오늘 포트폴리오 파일 존재: {today_path}")
        return today_path

    # 가장 최근 portfolio_YYMMDD.xlsx 찾기 (알파벳 정렬 = 날짜 정렬)
    pattern = os.path.join(reports_dir, 'portfolio_[0-9][0-9][0-9][0-9][0-9][0-9].xlsx')
    dated_files = sorted(glob.glob(pattern))

    if dated_files:
        source = dated_files[-1]
    else:
        # 날짜 파일이 없으면 r16 파일 사용
        source = get_portfolio_excel_path()
        if not source:
            logging.error("복사할 원본 포트폴리오 파일을 찾을 수 없습니다.")
            return None

    shutil.copy2(source, today_path)
    logging.info(f"포트폴리오 파일 생성: {today_path}  (원본: {os.path.basename(source)})")
    return today_path

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