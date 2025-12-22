# Please paste the content of CompanyManager.py here
import os
import sys
import logging
import time
import FinanceDataReader as fdr
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import datetime
from decimal import Decimal
from io import StringIO
from AppManager import get_db_connection

class CompanyManager:
    """
    주식 종목 정보를 관리하는 클래스
    """
    def __init__(self, db_access):
        """
        CompanyManager 생성자

        Args:
            db_access (dbaccess): 데이터베이스 접근을 위한 dbaccess 객체
        """
        self.db_access = db_access    
        
    def create_companies_table(self):
        """'companies' 테이블을 생성하는 메서드"""
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

    def create_daily_financials_table(self):
        """'daily_financials' 테이블을 생성하는 메서드"""
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

    def get_financial_data_from_fnguide(self, code, is_retry=False):
        """FnGuide에서 개별 종목의 재무 정보를 스크레이핑하는 메서드"""
        try:
            # fnguide page url prefix, postfix
            url_head = "https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A"
            url_tail = "&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
            url = f"{url_head}{code}{url_tail}"

            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=5)
            
            if "Snapshot 일부 종목에 한해" in response.text and not is_retry:
                logging.warning(f"FnGuide에서 '{code}'(우선주) 페이지를 찾을 수 없어 보통주로 재시도합니다.")
                common_stock_code = code[:-1] + '0'
                return self.get_financial_data_from_fnguide(common_stock_code, is_retry=True)

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
            
            # 우선주(보통 '0'으로 끝나지 않음)이고 테이블을 찾지 못했으며, 아직 재시도하지 않았다면 보통주로 재시도
            if not highlight_table and not is_retry and code[-1] != '0':
                logging.warning(f"'{code}'(우선주)에서 Financial Highlight 테이블을 찾지 못해 보통주로 재시도합니다.")
                common_stock_code = code[:-1] + '0'
                return self.get_financial_data_from_fnguide(common_stock_code, is_retry=True)

            if highlight_table:
                try:
                    df_list = pd.read_html(StringIO(str(highlight_table)), header=0)
                    if not df_list:
                        raise ValueError("pd.read_html did not find any tables.")
                    
                    df = df_list[0]
                    df = df.set_index(df.columns[0])

                    current_year_col_index = 5
                    next_year_col_index = 6

                    table_mapping = {
                        'PER': {'row_name': 'PER'},
                        'PBR': {'row_name': 'PBR'},
                        'EPS': {'row_name': 'EPS'},
                        'BPS': {'row_name': 'BPS'},
                        'ROE': {'row_name': 'ROE'},
                    }
                    
                    for key, mapping in table_mapping.items():
                        # 현재 값 추출
                        self._extract_from_table(df, data, key, mapping['row_name'], current_year_col_index, code)
                        # 예측 값 추출
                        self._extract_from_table(df, data, f"{key}_pred", mapping['row_name'], next_year_col_index, code)

                except Exception as e:
                    logging.warning(f"FnGuide '{code}' Financial Highlight 테이블 파싱 실패: {e}")
            else:
                logging.warning(f"FnGuide에서 '{code}' 종목의 Financial Highlight 테이블을 찾을 수 없습니다.")

            # 실적이슈 데이터 추출
            self._extract_performance_issues(soup, data)

            # 모든 키에 대해 값이 없으면 None으로 설정
            keys_to_check = [
                'EPS', 'ROE', 'BPS', 'IndustPER', 
                'PER_pred', 'PBR_pred', 'EPS_pred', 'ROE_pred', 'BPS_pred',
                'perf_yoy', 'perf_vs_3m_ago', 'perf_vs_consensus'
            ]
            for key in keys_to_check:
                if key not in data:
                    data[key] = None
            return data

        except Exception as e:
            logging.warning(f"FnGuide에서 '{code}' 종목의 재무 정보 스크레이핑 실패: {e}")
            return {}

    def _extract_from_table(self, df, data_dict, data_key, row_name, col_index, code):
        """Financial Highlight 테이블(DataFrame)에서 특정 값을 추출하는 헬퍼 함수"""
        # 컬럼 인덱스가 유효한지 확인
        if col_index >= len(df.columns):
            logging.debug(f"FnGuide '{code}': '{row_name}'에 대한 컬럼 인덱스 {col_index}가 범위를 벗어났습니다.")
            return

        try:
            # row_name을 포함하는 행의 이름을 찾음 (예: 'EPS(원)')
            target_row_name = next(ix for ix in df.index if row_name in ix)
        except StopIteration:
            logging.debug(f"FnGuide '{code}': Financial Highlight 테이블에서 '{row_name}'을(를) 포함하는 행을 찾을 수 없습니다.")
            return

        target_row = df.loc[target_row_name]
        if isinstance(target_row, pd.DataFrame):
            target_row = target_row.iloc[0]
        
        if col_index < len(target_row):
            value = target_row.iloc[col_index]
            if pd.notna(value):
                value_str = str(value).replace(',', '').replace('%', '').strip()
                if value_str and value_str not in ('-', 'N/A'):
                    try:
                        data_dict[data_key] = float(value_str)
                    except (ValueError, TypeError):
                        pass # 값을 float으로 변환할 수 없는 경우, 키가 설정되지 않음

    def _extract_numeric_value(self, soup, selector, data_dict, key):
        """CSS 선택자를 사용하여 HTML 요소에서 숫자 값을 추출하는 헬퍼 함수"""
        try:
            element = soup.select_one(selector)
            if element:
                text = element.text.strip()
                if text and text not in ('N/A', '-'):
                    # 쉼표 및 퍼센트 기호 제거
                    cleaned_text = text.replace(',', '').replace('%', '')
                    data_dict[key] = float(cleaned_text)
                else:
                    data_dict[key] = None
            else:
                data_dict[key] = None
        except (ValueError, TypeError) as e:
            logging.warning(f"'{selector}' 선택자를 사용하여 '{key}'에 대한 값을 구문 분석할 수 없습니다: {e}")
            data_dict[key] = None

    def _extract_performance_issues(self, soup, data_dict):
        """실적이슈 테이블에서 비교 데이터 추출"""
        try:
            issue_table = soup.select_one('#svdMainGrid2')
            if not issue_table:
                return
            
            # 테이블 데이터프레임으로 변환
            df_list = pd.read_html(StringIO(str(issue_table)))
            if not df_list:
                return
                
            df_issue = df_list[0]
            
            # 키워드로 필요한 컬럼 찾기
            column_keywords = {
                'perf_yoy': '전년동기대비',
                'perf_vs_3m_ago': '3개월전',
                'perf_vs_consensus': '예상실적대비'
            }
            
            for key, keyword in column_keywords.items():
                for col in df_issue.columns:
                    # 컬럼 이름은 튜플일 수 있으므로 문자열로 변환하여 확인
                    if keyword in str(col):
                        value = df_issue.loc[0, col] # 첫 번째 행(매출액 기준)의 값
                        if pd.notna(value) and str(value).strip() not in ('-', 'N/A'):
                            # 숫자인 경우에도 문자열로 저장
                            data_dict[key] = str(value).strip()
                        break # 해당 키워드에 대한 첫 번째 컬럼만 사용
        except Exception as e:
            logging.warning(f"실적이슈 데이터 추출 중 오류 발생: {e}")

    def save_companies_from_fdr(self, limit=None):
        """FinanceDataReader와 FnGuide를 사용하여 종목 정보를 가져와 DB에 분리하여 저장하는 메서드"""
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
                    # companies 테이블에 저장할 데이터
                    fnguide_url = f"https://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{row.get('Code')}&cID=&MenuYn=Y&ReportGB=&NewMenuID=101&stkGb=701"
                    companies_to_upsert.append((
                        row.get('Code'), row.get('Name'), row.get('Market'), fnguide_url
                    ))
                    
                    # daily_financials 테이블에 저장할 데이터
                    financials_to_insert.append((
                        row.get('Code'), today_date, row.get('Marcap'), row.get('Stocks'),
                        row.get('PBR'), row.get('PER'), row.get('IndustPER'), row.get('EPS'),
                        row.get('ROE'), row.get('DivRate'), row.get('BPS'),
                        row.get('PER_pred'), row.get('PBR_pred'), row.get('EPS_pred'),
                        row.get('ROE_pred'), row.get('BPS_pred'),
                        row.get('perf_yoy'), row.get('perf_vs_3m_ago'), row.get('perf_vs_consensus')
                    ))
                
                # 목표 개수(limit)를 채우면 중단
                if limit and len(companies_to_upsert) >= limit:
                    logging.info(f"조건을 만족하는 {limit}개의 종목을 찾았습니다. 목록 수집을 중단합니다.")
                    break

            # companies 테이블에 기본 정보 저장
            if companies_to_upsert:
                logging.info(f"총 {len(companies_to_upsert)}개 종목의 기본 정보를 데이터베이스에 저장 또는 업데이트합니다.")
                query_companies = """
                INSERT INTO companies (code, name, market, url) 
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    market = VALUES(market),
                    url = VALUES(url);
                """
                self.db_access.execute_many_query(query_companies, companies_to_upsert)
                logging.info(f"성공적으로 {len(companies_to_upsert)}개 종목 기본 정보를 저장/업데이트했습니다.")
            else:
                logging.info("저장하거나 업데이트할 종목 기본 정보가 없습니다.")

            # daily_financials 테이블에 재무 정보 저장
            if financials_to_insert:
                logging.info(f"총 {len(financials_to_insert)}개 종목의 일일 재무 정보를 데이터베이스에 저장합니다.")
                # (code, date)가 중복될 경우 최신 정보로 업데이트합니다.
                query_financials = """
                INSERT INTO daily_financials (
                    code, date, marcap, stocks, pbr, per, indust_per, eps, roe, div_yield, bps, 
                    per_pred, pbr_pred, eps_pred, roe_pred, bps_pred,
                    perf_yoy, perf_vs_3m_ago, perf_vs_consensus
                ) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    marcap = VALUES(marcap),
                    stocks = VALUES(stocks),
                    pbr = VALUES(pbr),
                    per = VALUES(per),
                    indust_per = VALUES(indust_per),
                    eps = VALUES(eps),
                    roe = VALUES(roe),
                    div_yield = VALUES(div_yield),
                    bps = VALUES(bps),
                    per_pred = VALUES(per_pred),
                    pbr_pred = VALUES(pbr_pred),
                    eps_pred = VALUES(eps_pred),
                    roe_pred = VALUES(roe_pred),
                    bps_pred = VALUES(bps_pred),
                    perf_yoy = VALUES(perf_yoy),
                    perf_vs_3m_ago = VALUES(perf_vs_3m_ago),
                    perf_vs_consensus = VALUES(perf_vs_consensus);
                """
                self.db_access.execute_many_query(query_financials, financials_to_insert)
                logging.info(f"성공적으로 {len(financials_to_insert)}개 종목의 일일 재무 정보를 저장했습니다.")
            else:
                logging.info("저장할 일일 재무 정보가 없습니다.")

        except Exception as e:
            logging.error(f"FDR 및 FnGuide에서 회사 정보를 저장하는 중 오류 발생: {e}")

if __name__ == "__main__":

    with get_db_connection() as db_access:
        logging.info("CompanyManager 스크립트 시작: %s", time.ctime())

        company_manager = CompanyManager(db_access)
        if company_manager.create_companies_table() and company_manager.create_daily_financials_table():
            logging.info("종목 정보 업데이트를 시작합니다.")
            
            # .env 파일에서 COMPANY_COUNT 가져오기
            company_count_str = os.getenv('STOCK_COUNT')
            limit = int(company_count_str) if company_count_str else None
            company_manager.save_companies_from_fdr(limit=limit)

        logging.info("CompanyManager 스크립트 종료: %s", time.ctime())