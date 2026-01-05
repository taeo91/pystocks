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

    def calculate_risk_metrics(self, code, months=3):
        """
        특정 종목의 최근 N개월간 리스크 지표(MDD, 최대 낙폭, 하락일 평균 하락폭)를 계산합니다.
        """
        try:
            start_date = datetime.date.today() - datetime.timedelta(days=months * 30)
            start_date_str = start_date.strftime('%Y-%m-%d')

            query = """
            WITH PeriodData AS (
                SELECT 
                    p.trade_date,
                    p.close_price,
                    p.high_price,
                    p.low_price,
                    LAG(p.close_price) OVER (ORDER BY p.trade_date) as prev_close,
                    -- 기간 내 누적 최고가 (MDD 계산용)
                    MAX(p.high_price) OVER (ORDER BY p.trade_date) as running_max_high
                FROM company_prices p
                JOIN companies c ON p.company_id = c.id
                WHERE c.code = %s
                  AND p.trade_date >= %s
            )
            SELECT 
                ROUND(MIN((low_price - running_max_high) / running_max_high * 100), 2) AS max_drawdown,
                ROUND(MIN((close_price - prev_close) / prev_close * 100), 2) AS worst_daily_drop,
                ROUND(AVG(CASE WHEN close_price < prev_close THEN (close_price - prev_close) / prev_close * 100 END), 2) AS avg_downside,
                ROUND(COUNT(CASE WHEN close_price < prev_close THEN 1 END) / COUNT(*) * 100, 2) AS down_prob
            FROM PeriodData
            WHERE prev_close IS NOT NULL;
            """
            
            logging.info(f"'{code}' 종목의 최근 {months}개월 리스크 지표를 계산합니다.")
            df = pd.read_sql(query, self.engine, params=(code, start_date_str))
            return df

        except Exception as e:
            logging.error(f"리스크 지표 계산 중 오류 발생: {e}")
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
        pass