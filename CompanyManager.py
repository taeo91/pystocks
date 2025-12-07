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
                market VARCHAR(50) COMMENT '시장'
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
                    columns = df.columns

                    # 사용자 요청: highlight_D_Y 테이블의 6번째 데이터 컬럼 값을 사용합니다.
                    # pandas DataFrame에서는 0부터 시작하므로 인덱스는 5가 됩니다.
                    target_col_index = 5

                    # 사용할 컬럼이 데이터프레임의 실제 컬럼 수보다 작은지 확인합니다.
                    if len(df.columns) <= target_col_index:
                        logging.warning(f"FnGuide '{code}': Financial Highlight 테이블에 6번째 데이터 컬럼이 존재하지 않습니다.")
                        # 컬럼이 없으면, 아래 로직에서 처리되지 않도록 -1로 설정합니다.
                        target_col_index = -1

                    table_mapping = {
                        'EPS': {'row_name': 'EPS', 'col_index': target_col_index},
                        'BPS': {'row_name': 'BPS', 'col_index': target_col_index},
                        'ROE': {'row_name': 'ROE', 'col_index': target_col_index},
                    }

                    for key, mapping in table_mapping.items():
                        base_row_name = mapping['row_name']
                        col_index = mapping['col_index']

                        if col_index == -1:
                            continue

                        # 'EPS'가 포함된 행(예: 'EPS(원)')을 찾습니다.
                        try:
                            target_row_name = next(ix for ix in df.index if base_row_name in ix)
                        except StopIteration:
                            logging.warning(f"FnGuide '{code}': Financial Highlight 테이블에서 '{base_row_name}'을 포함하는 행을 찾을 수 없습니다.")
                            continue

                        target_row = df.loc[target_row_name]
                        if isinstance(target_row, pd.DataFrame):
                            target_row = target_row.iloc[0]
                        
                        if col_index < len(target_row):
                            value = target_row.iloc[col_index]
                            if pd.notna(value):
                                value_str = str(value).replace(',', '').replace('%', '').strip()
                                if value_str and value_str not in ('-', 'N/A'):
                                    try:
                                        data[key] = float(value_str)
                                    except (ValueError, TypeError):
                                        pass
                except Exception as e:
                    logging.warning(f"FnGuide '{code}' Financial Highlight 테이블 파싱 실패: {e}")
            else:
                logging.warning(f"FnGuide에서 '{code}' 종목의 Financial Highlight 테이블을 찾을 수 없습니다.")

            # 모든 키에 대해 값이 없으면 None으로 설정
            for key in ['EPS', 'ROE', 'BPS', 'IndustPER']:
                if key not in data:
                    data[key] = None
            return data

        except Exception as e:
            logging.warning(f"FnGuide에서 '{code}' 종목의 재무 정보 스크레이핑 실패: {e}")
            return {}

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
                    companies_to_upsert.append((
                        row.get('Code'), row.get('Name'), row.get('Market')
                    ))
                    
                    # daily_financials 테이블에 저장할 데이터
                    financials_to_insert.append((
                        row.get('Code'), today_date, row.get('Marcap'), row.get('Stocks'),
                        row.get('PBR'), row.get('PER'), row.get('IndustPER'), row.get('EPS'),
                        row.get('ROE'), row.get('DivRate'), row.get('BPS')
                    ))
                
                # 목표 개수(limit)를 채우면 중단
                if limit and len(companies_to_upsert) >= limit:
                    logging.info(f"조건을 만족하는 {limit}개의 종목을 찾았습니다. 목록 수집을 중단합니다.")
                    break

            # companies 테이블에 기본 정보 저장
            if companies_to_upsert:
                logging.info(f"총 {len(companies_to_upsert)}개 종목의 기본 정보를 데이터베이스에 저장 또는 업데이트합니다.")
                query_companies = """
                INSERT INTO companies (code, name, market) 
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    market = VALUES(market);
                """
                self.db_access.execute_many_query(query_companies, companies_to_upsert)
                logging.info(f"성공적으로 {len(companies_to_upsert)}개 종목 기본 정보를 저장/업데이트했습니다.")
            else:
                logging.info("저장하거나 업데이트할 종목 기본 정보가 없습니다.")

            # daily_financials 테이블에 재무 정보 저장
            if financials_to_insert:
                logging.info(f"총 {len(financials_to_insert)}개 종목의 일일 재무 정보를 데이터베이스에 저장합니다.")
                # (code, date)가 중복될 경우 무시하고 넘어갑니다.
                query_financials = """
                INSERT IGNORE INTO daily_financials (code, date, marcap, stocks, pbr, per, indust_per, eps, roe, div_yield, bps) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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