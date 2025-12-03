import logging
import pandas as pd
import datetime
import os
from AppManager import get_db_connection
from sqlalchemy import create_engine

class Screener:
    """
    특정 조건에 맞는 종목을 검색하고 결과를 출력하는 클래스
    """
    def __init__(self, db_access):
        """
        Screener 생성자

        Args:
            db_access (dbaccess): 데이터베이스 접근을 위한 dbaccess 객체
        """
        self.db_access = db_access
        # pandas 경고를 해결하기 위해 SQLAlchemy 엔진 생성
        db_info = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME')
        }
        self.engine = create_engine(f"mysql+mysqlconnector://{db_info['user']}:{db_info['password']}@{db_info['host']}/{db_info['database']}")

    def find_golden_cross_stocks(self, days_ago=3):
        """
        지정된 기간 내에 MACD 골든크로스와 RSI 매수 신호가 동시에 발생한 종목을 찾습니다.

        Args:
            days_ago (int): 검색할 최근 일수

        Returns:
            pandas.DataFrame: 검색된 종목 정보 (종목코드, 종목명, 신호 발생일)
        """
        try:
            start_date = datetime.date.today() - datetime.timedelta(days=days_ago)
            start_date_str = start_date.strftime('%Y-%m-%d')

            query = """
            SELECT 
                c.code AS '종목코드',
                c.name AS '종목명',
                m.trade_date AS '신호발생일'
            FROM macd m
            JOIN rsi r ON m.company_id = r.company_id AND m.trade_date = r.trade_date
            JOIN companies c ON m.company_id = c.id
            WHERE 
                m.trade_date >= %s
                AND m.`cross` = 'GOLDEN'
                AND r.`signal` = 'BUY'
            ORDER BY m.trade_date DESC, c.name ASC;
            """
            
            logging.info(f"{start_date_str} 이후 MACD 골든크로스 & RSI 매수 신호 종목을 검색합니다.")
            df = pd.read_sql(query, self.engine, params=(start_date_str,))
            return df

        except Exception as e:
            logging.error(f"신호 종목 검색 중 오류 발생: {e}")
            return pd.DataFrame()

    def find_uptrend_stocks(self, period=20):
        """
        지정된 기간 동안 MACD와 RSI가 상승 추세인 종목을 찾습니다.
        (현재 값 > 기간 전 값)

        Args:
            period (int): 추세를 확인할 거래일 기간

        Returns:
            pandas.DataFrame: 검색된 종목 정보
        """
        try:
            # 충분한 데이터를 확보하기 위해 기간의 2배 정도를 조회
            start_date = datetime.date.today() - datetime.timedelta(days=period * 2)
            start_date_str = start_date.strftime('%Y-%m-%d')

            # LAG 윈도우 함수를 사용하여 N 거래일 전의 데이터를 가져오는 쿼리
            # period - 1 을 사용하는 이유는 LAG(col, N)이 N행 이전의 값을 가져오기 때문입니다.
            # (예: LAG(col, 19)는 20번째 거래일의 값을 가져옴)
            query = f"""
            WITH ranked_indicators AS (
                SELECT
                    m.company_id,
                    m.trade_date,
                    c.code,
                    c.name,
                    m.macd,
                    r.rsi,
                    LAG(m.macd, {period - 1}) OVER (PARTITION BY m.company_id ORDER BY m.trade_date) as past_macd,
                    LAG(r.rsi, {period - 1}) OVER (PARTITION BY m.company_id ORDER BY m.trade_date) as past_rsi,
                    ROW_NUMBER() OVER (PARTITION BY m.company_id ORDER BY m.trade_date DESC) as rn
                FROM macd m
                JOIN rsi r ON m.company_id = r.company_id AND m.trade_date = r.trade_date
                JOIN companies c ON m.company_id = c.id
                WHERE m.trade_date >= %s
            )
            SELECT
                code AS '종목코드',
                name AS '종목명',
                trade_date AS '최근거래일',
                past_macd AS '{period}일전_MACD',
                macd AS '최근_MACD',
                past_rsi AS '{period}일전_RSI',
                rsi AS '최근_RSI'
            FROM ranked_indicators
            WHERE rn = 1 AND macd > past_macd AND rsi > past_rsi
            ORDER BY name ASC;
            """
            
            logging.info(f"최근 {period}일간 MACD & RSI 상승 추세 종목을 검색합니다.")
            df = pd.read_sql(query, self.engine, params=(start_date_str,))
            return df

        except Exception as e:
            logging.error(f"상승 추세 종목 검색 중 오류 발생: {e}")
            return pd.DataFrame()


def export_to_excel(dataframe, filename='golden_cross_stocks.xlsx'):
    """데이터프레임을 엑셀 파일로 저장합니다."""
    if dataframe.empty:
        logging.info("엑셀로 저장할 데이터가 없습니다.")
        return

    try:
        # reports 디렉터리가 없으면 생성
        if not os.path.exists('reports'):
            os.makedirs('reports')
        
        filepath = os.path.join('reports', filename)
        dataframe.to_excel(filepath, index=False)
        logging.info(f"검색 결과가 '{filepath}' 파일로 저장되었습니다.")
    except Exception as e:
        logging.error(f"엑셀 파일 저장 중 오류 발생: {e}")

if __name__ == "__main__":
    with get_db_connection() as db_access:
        screener = Screener(db_access)
        # 최근 20일간 상승 추세인 종목 검색
        uptrend_stocks = screener.find_uptrend_stocks(period=20)
        export_to_excel(uptrend_stocks, filename='uptrend_stocks_20d.xlsx')