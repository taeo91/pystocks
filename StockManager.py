import os
import logging
import time
import datetime
import requests
import re
from decimal import Decimal
from io import StringIO
import pandas as pd
from bs4 import BeautifulSoup
import FinanceDataReader as fdr
from AppManager import get_db_connection
from IndicatorManager import IndicatorManager

class StockManager:
    """
    주식 종목 정보(기본정보, 재무제표)와 가격 정보(시세)를 통합 관리하는 클래스
    """
    def __init__(self, db_access):
        """
        StockManager 생성자
        Args:
            db_access (dbaccess): 데이터베이스 접근 객체
        """
        self.db_access = db_access
        self.indicator_manager = IndicatorManager(db_access)

    def create_tables(self):
        """주식 관련 테이블(companies, daily_financials, prices)을 생성"""
        if self._create_companies_table() and self._create_daily_financials_table() and self._create_prices_table():
            return True
        return False

    def _create_companies_table(self):
        """'companies' 테이블 생성"""
        try:
            query = """
            CREATE TABLE IF NOT EXISTS companies (
                id INT AUTO_INCREMENT PRIMARY KEY,
                code VARCHAR(20) NOT NULL UNIQUE COMMENT '종목코드',
                name VARCHAR(255) NOT NULL COMMENT '종목명',
                market VARCHAR(50) COMMENT '시장',
                url VARCHAR(255) COMMENT 'FnGuide URL'
            ) COMMENT '종목 기본 정보';
            """
            self.db_access.execute_query(query)
            logging.info("Table 'companies' created or already exists.")
            return True 
        except Exception as e:
            logging.error(f"Error creating 'companies' table: {e}")
            return False

    def _create_daily_financials_table(self):
        """'daily_financials' 테이블 생성"""
        try:
            query = """
            CREATE TABLE IF NOT EXISTS daily_financials (
                id INT AUTO_INCREMENT PRIMARY KEY,
                code VARCHAR(20) NOT NULL COMMENT '종목코드',
                date DATE NOT NULL COMMENT '기준일자',
                marcap BIGINT COMMENT '시가총액(억)',
                stocks BIGINT COMMENT '발행주식수',
                pbr DECIMAL(10, 2) COMMENT 'PBR',
                per DECIMAL(10, 2) COMMENT 'PER',
                indust_per DECIMAL(10, 2) COMMENT '업종PER',
                eps DECIMAL(15, 2) COMMENT 'EPS',
                roe DECIMAL(10, 2) COMMENT 'ROE',
                div_yield DECIMAL(10, 2) COMMENT '배당수익률(%)',
                bps DECIMAL(15, 2) COMMENT '주당순자산가치',
                per_pred DECIMAL(10, 2) COMMENT 'PER(예상)',
                pbr_pred DECIMAL(10, 2) COMMENT 'PBR(예상)',
                eps_pred DECIMAL(15, 2) COMMENT 'EPS(예상)',
                roe_pred DECIMAL(10, 2) COMMENT 'ROE(예상)',
                bps_pred DECIMAL(15, 2) COMMENT 'BPS(예상)',
                max_drawdown DECIMAL(10, 2) COMMENT '최대낙폭(1년)',
                avg_drawdown DECIMAL(10, 2) COMMENT '평균낙폭(1년)',
                perf_yoy VARCHAR(50) COMMENT '실적이슈(전년동기대비)',
                perf_vs_3m_ago VARCHAR(50) COMMENT '실적이슈(3개월전대비)',
                perf_vs_consensus VARCHAR(50) COMMENT '실적이슈(예상실적대비)',
                FOREIGN KEY (code) REFERENCES companies (code) ON DELETE CASCADE,
                UNIQUE KEY (code, date)
            ) COMMENT '일일 재무 정보';
            """
            self.db_access.execute_query(query)
            logging.info("Table 'daily_financials' created or already exists.")
            return True
        except Exception as e:
            logging.error(f"Error creating 'daily_financials' table: {e}")
            return False

    def _create_prices_table(self):
        """'company_prices' 테이블 생성"""
        try:
            query = """
            CREATE TABLE IF NOT EXISTS company_prices (
                price_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                company_id INT NOT NULL,
                trade_date DATE NOT NULL,
                open_price DECIMAL(15, 2) NULL,
                high_price DECIMAL(15, 2) NULL,
                low_price DECIMAL(15, 2) NULL,
                close_price DECIMAL(15, 2) NULL,
                volume BIGINT,
                FOREIGN KEY (company_id) REFERENCES companies(id),
                UNIQUE KEY (company_id, trade_date)
            )
            """
            self.db_access.execute_query(query)
            logging.info("Table 'company_prices' created or already exists.")
            return True
        except Exception as e:
            logging.error(f"Error creating 'company_prices' table: {e}")
            return False

    def save_stock_info(self, limit=None):
        """FinanceDataReader와 FnGuide를 사용하여 종목 정보를 가져와 DB에 저장"""
        try:
            logging.info("KRX에서 전체 종목 목록을 가져옵니다...")
            stocks = fdr.StockListing('KRX')
            stocks = stocks.sort_values(by='Marcap', ascending=False)
            
            today_date = datetime.date.today()
            companies_to_upsert = []
            financials_to_insert = []
            
            logging.info("조건에 맞는 종목을 필터링하고 DB에 저장할 데이터를 준비합니다...")
            for _, row in stocks.iterrows():
                # 상세 재무 정보 스크레이핑
                fnguide_financials = self.get_financial_data_from_fnguide(row.get('Code'))
                
                # FDR 데이터와 스크래핑 데이터 병합
                if fnguide_financials:
                    for key, fnguide_val in fnguide_financials.items():
                        if fnguide_val is not None:
                            row[key] = fnguide_val
                
                # 조건 필터링: PER > 0 이고, EPS >= 0 인 종목만 선택
                per_value = row.get('PER')
                eps_value = row.get('EPS')

                if per_value is not None and pd.notna(per_value) and per_value > 0 and eps_value is not None and pd.notna(eps_value) and eps_value >= 0:
                    # companies 테이블 데이터
                    fnguide_url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{row.get('Code')}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
                    companies_to_upsert.append((
                        row.get('Code'), row.get('Name'), row.get('Market'), fnguide_url
                    ))
                    
                    # daily_financials 테이블 데이터
                    financials_to_insert.append((
                        row.get('Code'), today_date, row.get('Marcap'), row.get('Stocks'),
                        row.get('PBR'), row.get('PER'), row.get('IndustPER'), row.get('EPS'),
                        row.get('ROE'), row.get('DivRate'), row.get('BPS'),
                        row.get('PER_pred'), row.get('PBR_pred'), row.get('EPS_pred'),
                        row.get('ROE_pred'), row.get('BPS_pred'),
                        row.get('perf_yoy'), row.get('perf_vs_3m_ago'), row.get('perf_vs_consensus')
                    ))
                
                if limit and len(companies_to_upsert) >= limit:
                    logging.info(f"조건을 만족하는 {limit}개의 종목을 찾았습니다. 목록 수집을 중단합니다.")
                    break

            # DB 저장 로직
            if companies_to_upsert:
                query_companies = """
                INSERT INTO companies (code, name, market, url) 
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name), market = VALUES(market), url = VALUES(url);
                """
                self.db_access.execute_many_query(query_companies, companies_to_upsert)
                logging.info(f"성공적으로 {len(companies_to_upsert)}개 종목 기본 정보를 저장/업데이트했습니다.")

            if financials_to_insert:
                query_financials = """
                INSERT INTO daily_financials (
                    code, date, marcap, stocks, pbr, per, indust_per, eps, roe, div_yield, bps, 
                    per_pred, pbr_pred, eps_pred, roe_pred, bps_pred,
                    perf_yoy, perf_vs_3m_ago, perf_vs_consensus
                ) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    marcap = VALUES(marcap), stocks = VALUES(stocks), pbr = VALUES(pbr), per = VALUES(per),
                    indust_per = VALUES(indust_per), eps = VALUES(eps), roe = VALUES(roe), div_yield = VALUES(div_yield),
                    bps = VALUES(bps), per_pred = VALUES(per_pred), pbr_pred = VALUES(pbr_pred),
                    eps_pred = VALUES(eps_pred), roe_pred = VALUES(roe_pred), bps_pred = VALUES(bps_pred),
                    perf_yoy = VALUES(perf_yoy), perf_vs_3m_ago = VALUES(perf_vs_3m_ago), perf_vs_consensus = VALUES(perf_vs_consensus);
                """
                self.db_access.execute_many_query(query_financials, financials_to_insert)
                logging.info(f"성공적으로 {len(financials_to_insert)}개 종목의 일일 재무 정보를 저장했습니다.")

        except Exception as e:
            logging.error(f"FDR 및 FnGuide에서 회사 정보를 저장하는 중 오류 발생: {e}")

    def save_daily_prices(self, start_date=None, limit=None):
        """DB에 저장된 모든 회사에 대해 일별 주가 데이터를 가져와 저장"""
        try:
            cursor = self.db_access.connection.cursor(dictionary=True)
            
            company_query = "SELECT id, code FROM companies"
            if limit:
                company_query += f" LIMIT {limit}"
            cursor.execute(company_query)
            companies_to_fetch = cursor.fetchall()

            if start_date is None:
                start_date = (datetime.date.today() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')

            logging.info(f"Fetching price data for {len(companies_to_fetch)} companies. Default start date: {start_date}.")

            for company in companies_to_fetch:
                company_id = company['id']
                stock_code = company['code']
                
                last_date_query = "SELECT MAX(trade_date) FROM company_prices WHERE company_id = %s"
                cursor.execute(last_date_query, (company_id,))
                last_date_result = cursor.fetchone()
                
                fetch_start_date = start_date
                if last_date_result and last_date_result.get('MAX(trade_date)'):
                    fetch_start_date = last_date_result['MAX(trade_date)'] + datetime.timedelta(days=1)
                    fetch_start_date = fetch_start_date.strftime('%Y-%m-%d')

                try:
                    df = fdr.DataReader(stock_code, start=fetch_start_date)
                    if df.empty:
                        continue

                    data_to_insert = [
                        (company_id, trade_date.strftime('%Y-%m-%d'), row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])
                        for trade_date, row in df.iterrows()
                    ]
                    
                    if data_to_insert:
                        price_query = """
                        INSERT INTO company_prices (company_id, trade_date, open_price, high_price, low_price, close_price, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                            open_price=VALUES(open_price), high_price=VALUES(high_price), low_price=VALUES(low_price), 
                            close_price=VALUES(close_price), volume=VALUES(volume)
                        """
                        insert_cursor = self.db_access.connection.cursor()
                        insert_cursor.executemany(price_query, data_to_insert)
                        self.db_access.connection.commit()
                        insert_cursor.close()                        

                except Exception as e:
                    if '404' in str(e):
                        logging.warning(f"'{stock_code}' 종목 데이터를 찾을 수 없어(404) DB에서 삭제를 시도합니다.")
                        try:
                            self.db_access.execute_query("DELETE FROM company_prices WHERE company_id = %s", (company_id,))
                            self.db_access.execute_query("DELETE FROM companies WHERE id = %s", (company_id,))
                        except Exception as delete_e:
                            logging.error(f"'{stock_code}' 종목 DB 삭제 중 오류 발생: {delete_e}")
                    else:
                        logging.error(f"'{stock_code}' 종목 데이터 처리 중 오류 발생: {e}")

            cursor.close()
            logging.info("주가 데이터 저장 완료.")
        except Exception as e:
            logging.error(f"An unexpected error occurred in save_daily_prices: {e}")

    def update_all_indicators(self, limit=None):
        """DB에 저장된 모든 회사에 대해 기술적 지표를 계산하고 저장"""
        try:
            cursor = self.db_access.connection.cursor(dictionary=True)
            company_query = "SELECT id, code FROM companies"
            if limit:
                company_query += f" LIMIT {limit}"
            cursor.execute(company_query)
            companies_to_update = cursor.fetchall()
            cursor.close()

            logging.info(f"기술적 지표 계산을 시작합니다. 대상 종목 수: {len(companies_to_update)}")

            for company in companies_to_update:
                self.indicator_manager.calculate_and_save_indicators(company['id'])
        
        except Exception as e:
            logging.error(f"An unexpected error occurred in update_all_indicators: {e}")

    def update_risk_metrics(self, limit=None):
        """DB에 저장된 주가 정보를 바탕으로 최대낙폭/평균낙폭을 계산하여 daily_financials에 업데이트"""
        try:
            logging.info("종목별 리스크 지표(MDD, 평균낙폭) 계산 및 업데이트 시작...")
            
            cursor = self.db_access.connection.cursor(dictionary=True)
            company_query = "SELECT id, code FROM companies"
            if limit:
                company_query += f" LIMIT {limit}"
            cursor.execute(company_query)
            companies = cursor.fetchall()
            cursor.close()
   
            start_date = (datetime.date.today() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')
            today_date = datetime.date.today()

            for company in companies:
                company_id = company['id']
                code = company['code']

                query = """
                SELECT trade_date, close_price, high_price, low_price
                FROM company_prices
                WHERE company_id = %s AND trade_date >= %s
                ORDER BY trade_date ASC
                """
                prices = self.db_access.fetch_all(query, (company_id, start_date))
                
                if not prices:
                    continue

                df = pd.DataFrame(prices, columns=['trade_date', 'close_price', 'high_price', 'low_price'])
                df[['close_price', 'high_price', 'low_price']] = df[['close_price', 'high_price', 'low_price']].astype(float)

                # Running Max (High 기준)
                df['running_max'] = df['high_price'].cummax()
                
                # MDD: ((Low - Running_Max) / Running_Max) * 100 의 최솟값
                df['dd_low'] = (df['low_price'] - df['running_max']) / df['running_max'] * 100
                max_drawdown = df['dd_low'].min()
                
                # Avg Drawdown: ((Close - Running_Max) / Running_Max) * 100 의 평균
                df['dd_close'] = (df['close_price'] - df['running_max']) / df['running_max'] * 100
                avg_drawdown = df['dd_close'].mean()

                update_query = "UPDATE daily_financials SET max_drawdown = %s, avg_drawdown = %s WHERE code = %s AND date = %s"
                self.db_access.execute_query(update_query, (max_drawdown, avg_drawdown, code, today_date))
            
            logging.info("리스크 지표 업데이트 완료.")
        except Exception as e:
            logging.error(f"리스크 지표 업데이트 중 오류 발생: {e}")

    # --- FnGuide Scraping Helpers ---
    def get_financial_data_from_fnguide(self, code, is_retry=False):
        try:
            url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=5)
            
            if "Snapshot 일부 종목에 한해" in response.text and not is_retry:
                return self.get_financial_data_from_fnguide(code[:-1] + '0', is_retry=True)

            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            data = {}

            summary_mapping = [
                ('div.corp_group2 > dl:nth-of-type(1) > dd', 'PER'),
                ('div.corp_group2 > dl:nth-of-type(2) > dd', 'IndustPER'),
                ('div.corp_group2 > dl:nth-of-type(4) > dd', 'PBR'),
                ('div.corp_group2 > dl:nth-of-type(5) > dd', 'DivRate')
            ]
            for selector, key in summary_mapping:
                self._extract_numeric_value(soup, selector, data, key)

            highlight_table = soup.select_one('#highlight_D_Y')
            if not highlight_table and not is_retry and code[-1] != '0':
                return self.get_financial_data_from_fnguide(code[:-1] + '0', is_retry=True)

            if highlight_table:
                try:
                    df_list = pd.read_html(StringIO(str(highlight_table)), header=0)
                    if df_list:
                        df = df_list[0].set_index(df_list[0].columns[0])
                        table_mapping = {'PER': 'PER', 'PBR': 'PBR', 'EPS': 'EPS', 'BPS': 'BPS', 'ROE': 'ROE'}
                        for key, row_name in table_mapping.items():
                            self._extract_from_table(df, data, key, row_name, 5, code)
                            self._extract_from_table(df, data, f"{key}_pred", row_name, 6, code)
                except Exception:
                    pass

            self._extract_performance_issues(soup, data)
            return data
        except Exception:
            return {}

    def _extract_from_table(self, df, data_dict, data_key, row_name, col_index, code):
        try:
            target_row_name = next(ix for ix in df.index if row_name in ix)
            target_row = df.loc[target_row_name]
            if isinstance(target_row, pd.DataFrame): target_row = target_row.iloc[0]
            if col_index < len(target_row):
                value = target_row.iloc[col_index]
                if pd.notna(value):
                    value_str = str(value).replace(',', '').replace('%', '').strip()
                    if value_str and value_str not in ('-', 'N/A'):
                        try: data_dict[data_key] = float(value_str)
                        except: pass
        except: pass

    def _extract_numeric_value(self, soup, selector, data_dict, key):
        try:
            element = soup.select_one(selector)
            if element:
                text = element.text.strip().replace(',', '').replace('%', '')
                if text and text not in ('N/A', '-'):
                    data_dict[key] = float(text)
        except: data_dict[key] = None

    def _extract_performance_issues(self, soup, data_dict):
        try:
            issue_table = soup.select_one('#svdMainGrid2')
            if not issue_table: return
            df_list = pd.read_html(StringIO(str(issue_table)))
            if not df_list: return
            df_issue = df_list[0]
            keywords = {'perf_yoy': '전년동기대비', 'perf_vs_3m_ago': '3개월전', 'perf_vs_consensus': '예상실적대비'}
            for key, keyword in keywords.items():
                for col in df_issue.columns:
                    if keyword in str(col):
                        val = df_issue.loc[0, col]
                        if pd.notna(val) and str(val).strip() not in ('-', 'N/A'):
                            data_dict[key] = str(val).strip()
                        break
        except: pass

if __name__ == "__main__":
    with get_db_connection() as db_access:
        logging.basicConfig(level=logging.INFO)
        logging.info("StockManager 스크립트 시작: %s", time.ctime())
        
        stock_manager = StockManager(db_access)
        if stock_manager.create_tables() and stock_manager.indicator_manager.create_indicators_tables():
            stock_count_str = os.getenv('STOCK_COUNT')
            limit = int(stock_count_str) if stock_count_str else None
            
            stock_manager.save_stock_info(limit=limit)
            
            start_date = os.getenv('PRICE_FETCH_START_DATE')
            stock_manager.save_daily_prices(start_date=start_date, limit=limit)
            stock_manager.update_all_indicators(limit=limit)
            stock_manager.update_risk_metrics(limit=limit)
            
        logging.info("StockManager 스크립트 종료: %s", time.ctime())