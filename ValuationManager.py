import logging
import datetime
import os
import pandas as pd
from AppManager import get_db_connection

class ValuationManager:
    """
    주식 가치 평가를 관리하는 클래스
    """
    # 가치 평가를 위한 기준값
    REQUIRED_ROE = 8.0  # 최소 요구 ROE (%)
    GROWTH_PREMIUM_CAP = 0.5  # EPS 성장률 프리미엄 최대치 (50%)
    LOW_VALUATION_THRESHOLD = -10.0  # 저평가 기준 괴리율 (%)
    HIGH_VALUATION_THRESHOLD = 10.0  # 고평가 기준 괴리율 (%)

    # 실적 이슈 조정 계수 맵
    PERFORMANCE_ADJUSTMENT_MAP = {
        '상회': 1.1, '하회': 0.9, '유지': 1.0,
        '컨센상회': 1.1, '컨센하회': 0.9
    }

    def __init__(self, db_access):
        """
        ValuationManager 생성자

        Args:
            db_access: 데이터베이스 접근 객체
        """
        self.db_access = db_access

    def create_valuation_table(self):
        """'valuations' 테이블을 생성하는 메서드"""
        try:
            query = """
            CREATE TABLE IF NOT EXISTS valuations (
                id INT AUTO_INCREMENT PRIMARY KEY,
                code VARCHAR(20) NOT NULL COMMENT '종목코드',
                date DATE NOT NULL COMMENT '평가일자',
                fair_value DECIMAL(15, 2) COMMENT '적정주가',
                current_price DECIMAL(15, 2) COMMENT '현재주가',
                discrepancy_ratio DECIMAL(10, 4) COMMENT '괴리율 (%)',
                eps_growth_rate DECIMAL(10, 4) COMMENT 'EPS 성장률 (%)',
                valuation_result VARCHAR(50) COMMENT '평가결과 (저평가/고평가)',
                FOREIGN KEY (code) REFERENCES companies (code) ON DELETE CASCADE,
                UNIQUE KEY (code, date)
            ) COMMENT '일일 주식 가치 평가 정보';
            """
            self.db_access.execute_query(query)
            logging.info("Table 'valuations' created or already exists.")
            return True
        except Exception as e:
            logging.error(f"Error creating 'valuations' table: {e}")
            return False

    def calculate_and_save_valuations(self, limit=None):
        """DB에 저장된 모든 종목의 가치를 평가하고 결과를 엑셀 파일로 저장합니다."""
        try:
            logging.info("가치 평가를 시작합니다...")
            today = datetime.date.today()
            today_str = today.strftime('%Y-%m-%d')

            all_data = self._fetch_valuation_data_bulk(today_str, limit)
            if not all_data:
                logging.warning(f"'{today_str}' 날짜로 평가할 데이터가 없습니다.")
                return

            valuation_results = []
            for stock_data in all_data:
                # 필수 데이터 확인 (None 이면 계산 불가)
                required_keys = ['code', 'roe_pred', 'bps_pred', 'current_price']
                if not all(stock_data.get(key) is not None for key in required_keys):
                    logging.debug(f"'{stock_data.get('code')}'에 대한 충분한 데이터가 없어 평가를 건너뜁니다.")
                    continue

                result = self._perform_valuation_calculation(stock_data)
                if result:
                    valuation_results.append(result)

            if valuation_results:
                logging.info(f"총 {len(valuation_results)}개 종목의 가치 평가를 완료했습니다. Excel 파일로 저장합니다.")
                self._save_results_to_excel(valuation_results, today_str)
            else:
                logging.info("가치 평가를 수행할 종목이 없습니다.")

        except Exception as e:
            logging.error(f"가치 평가 계산 및 저장 중 오류 발생: {e}")

    def calculate_valuation(self, code, date_str):
        """
        개별 종목의 가치를 평가하는 메서드 (단일 종목용).
        여러 종목을 평가할 때는 성능이 최적화된 calculate_and_save_valuations() 사용을 권장합니다.
        """
        try:
            # 1. DB에서 필요한 재무 정보 및 현재가 가져오기
            # 참고: 이 메서드는 단일 종목에 대해 여러 쿼리를 실행하므로 대량 처리에는 비효율적입니다.
            query_financials = """
            SELECT eps, eps_pred, roe_pred, bps_pred, perf_yoy, perf_vs_3m_ago, perf_vs_consensus
            FROM daily_financials 
            WHERE code = %s AND date = %s
            """
            financial_data_tuple = self.db_access.fetch_one(query_financials, (code, date_str))
            if not financial_data_tuple or any(d is None for d in financial_data_tuple[1:4]): # eps_pred, roe_pred, bps_pred
                logging.debug(f"'{code}'에 대한 충분한 재무 데이터가 없습니다.")
                return None

            fin_cols = ['eps', 'eps_pred', 'roe_pred', 'bps_pred', 'perf_yoy', 'perf_vs_3m_ago', 'perf_vs_consensus']
            financial_data = dict(zip(fin_cols, financial_data_tuple))

            query_price = """
            SELECT p.close_price 
            FROM prices p
            JOIN companies c ON p.company_id = c.id
            WHERE c.code = %s
            ORDER BY p.trade_date DESC LIMIT 1
            """
            price_data = self.db_access.fetch_one(query_price, (code,))
            if not price_data or price_data[0] is None:
                logging.debug(f"'{code}'에 대한 가격 정보가 없습니다.")
                return None

            # 2. 데이터 취합 및 중앙 계산 메서드 호출
            stock_data = {
                'code': code,
                'current_price': price_data[0],
                **financial_data
            }

            return self._perform_valuation_calculation(stock_data)

        except Exception as e:
            logging.error(f"'{code}' 가치 평가 중 오류: {e}")
            return None

    def _fetch_valuation_data_bulk(self, date_str, limit=None):
        """평가에 필요한 모든 종목의 재무 및 가격 데이터를 한 번의 쿼리로 가져옵니다."""
        # 참고: 이 쿼리는 MySQL 8.0+ 또는 ROW_NUMBER()를 지원하는 DB에서 최적의 성능을 보입니다.
        # DB가 윈도우 함수를 지원하지 않는 경우, 서브쿼리 등으로 최신 가격을 가져오는 로직 수정이 필요합니다.
        query = """
            SELECT
                df.code, df.eps, df.eps_pred, df.roe_pred, df.bps_pred,
                df.perf_yoy, df.perf_vs_3m_ago, df.perf_vs_consensus,
                p_latest.close_price AS current_price
            FROM
                daily_financials AS df
            JOIN
                companies AS c ON df.code = c.code
            JOIN
                (
                    SELECT
                        company_id,
                        close_price,
                        ROW_NUMBER() OVER(PARTITION BY company_id ORDER BY trade_date DESC) as rn
                    FROM prices
                ) AS p_latest ON c.id = p_latest.company_id AND p_latest.rn = 1
            WHERE
                df.date = %s
        """
        params = [date_str]
        if limit:
            query += " LIMIT %s"
            params.append(int(limit))

        # fetch_all이 파라미터를 지원하고, 결과가 튜플 리스트라고 가정합니다.
        rows = self.db_access.fetch_all(query, params)
        if not rows:
            return []

        columns = [
            'code', 'eps', 'eps_pred', 'roe_pred', 'bps_pred',
            'perf_yoy', 'perf_vs_3m_ago', 'perf_vs_consensus', 'current_price'
        ]
        return [dict(zip(columns, row)) for row in rows]

    def _perform_valuation_calculation(self, stock_data):
        """
        주어진 데이터를 바탕으로 개별 종목의 가치를 평가합니다. (RIM 기반)
        적정주가 = BPS + ( (ROE - 요구수익률) / 요구수익률 ) * BPS
        """
        try:
            # 데이터 추출
            code = stock_data['code']
            current_price = stock_data['current_price']
            eps, eps_pred = stock_data.get('eps'), stock_data.get('eps_pred')
            roe_pred, bps_pred = stock_data.get('roe_pred'), stock_data.get('bps_pred')
            perf_yoy, perf_vs_3m_ago, perf_vs_consensus = (
                stock_data.get('perf_yoy'), stock_data.get('perf_vs_3m_ago'), stock_data.get('perf_vs_consensus')
            )

            # EPS 성장률
            eps_growth_rate = 0
            if eps and eps > 0 and eps_pred is not None:
                eps_growth_rate = ((eps_pred - eps) / eps) * 100

            # RIM 모델 기반 적정주가 계산
            fair_value = 0
            if roe_pred is not None and roe_pred >= 0 and bps_pred is not None and bps_pred > 0:
                excess_profit_per_share = (roe_pred / 100 - self.REQUIRED_ROE / 100) * bps_pred
                fair_value = bps_pred + (excess_profit_per_share / (self.REQUIRED_ROE / 100))

                # EPS 성장률이 높으면 프리미엄 부여
                growth_premium = min(max(eps_growth_rate / 100, 0), self.GROWTH_PREMIUM_CAP)
                fair_value *= (1 + growth_premium)

            if fair_value <= 0:
                return None

            # 실적 이슈 기반 조정 계수 적용
            performance_adjustment = 1.0
            if perf_yoy:
                performance_adjustment *= self.PERFORMANCE_ADJUSTMENT_MAP.get(perf_yoy, 1.0)
            if perf_vs_3m_ago:
                performance_adjustment *= self.PERFORMANCE_ADJUSTMENT_MAP.get(perf_vs_3m_ago, 1.0)
            if perf_vs_consensus:
                performance_adjustment *= self.PERFORMANCE_ADJUSTMENT_MAP.get(perf_vs_consensus, 1.0)

            adjusted_fair_value = fair_value * performance_adjustment

            # 괴리율 및 평가 결과
            discrepancy_ratio = ((current_price - adjusted_fair_value) / adjusted_fair_value) * 100
            if discrepancy_ratio < self.LOW_VALUATION_THRESHOLD:
                result = "저평가"
            elif discrepancy_ratio > self.HIGH_VALUATION_THRESHOLD:
                result = "고평가"
            else:
                result = "적정"

            return {
                'code': code,
                'current_price': current_price,
                'fair_value': round(adjusted_fair_value, 2),
                'discrepancy_ratio': round(discrepancy_ratio, 2),
                'eps_growth_rate': round(eps_growth_rate, 2),
                'result': result,
                'base_fair_value': round(fair_value, 2),
                'perf_adj_factor': round(performance_adjustment, 2),
                'perf_yoy': perf_yoy,
                'perf_vs_3m_ago': perf_vs_3m_ago,
                'perf_vs_consensus': perf_vs_consensus
            }

        except Exception as e:
            logging.error(f"'{stock_data.get('code')}' 가치 평가 계산 중 오류: {e}")
            return None

    def _save_results_to_excel(self, results, date_str):
        """평가 결과를 Excel 파일로 저장합니다."""
        if not results:
            logging.info("저장할 가치 평가 결과가 없습니다.")
            return

        try:
            df = pd.DataFrame(results)
            report_dir = 'reports'
            os.makedirs(report_dir, exist_ok=True)
            filename = os.path.join(report_dir, f'valuation_results_{date_str}.xlsx')
            df.to_excel(filename, index=False, engine='openpyxl')
            logging.info(f"가치 평가 결과를 '{filename}' 파일로 성공적으로 저장했습니다.")
        except Exception as e:
            logging.error(f"Excel 파일 저장 중 오류 발생: {e}")

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    with get_db_connection() as db:
        valuation_manager = ValuationManager(db)
        # 모든 종목에 대해 가치 평가 실행
        valuation_manager.calculate_and_save_valuations()

    logging.info("ValuationManager 스크립트 실행 완료.")
