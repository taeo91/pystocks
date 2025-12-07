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
from decimal import Decimal
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
                marcap BIGINT COMMENT '시가총액(억)',
                stocks BIGINT COMMENT '발행주식수',
                pbr DECIMAL(10, 2) COMMENT 'PBR',
                per DECIMAL(10, 2) COMMENT 'PER',
                eps DECIMAL(15, 2) COMMENT 'EPS',
                roe DECIMAL(10, 2) COMMENT 'ROE',
                div_yield DECIMAL(10, 2) COMMENT '배당수익률(%)',
                bps DECIMAL(15, 2) COMMENT '주당순자산가치'
            ) COMMENT '종목 정보';
            """
            self.db_access.execute_query(query)
            logging.info("Table 'companies' created or already exists.")
            return True 
        except Exception as e:
            logging.error(f"Error creating 'companies' table: {e}")
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
            
            # 일부 우선주 종목은 페이지가 없을 수 있음. 이 경우 보통주로 재시도.
            if "Snapshot 일부 종목에 한해" in response.text and not is_retry:
                logging.warning(f"FnGuide에서 '{code}'(우선주) 페이지를 찾을 수 없어 보통주로 재시도합니다.")
                common_stock_code = code[:-1] + '0'
                return self.get_financial_data_from_fnguide(common_stock_code, is_retry=True)

            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            data = {}

            # --- 상단 요약 정보 스크레이핑 ---
            summary_mapping = [
                ('div.corp_group2 > dl:nth-of-type(1) > dd', 'PER'),
                ('div.corp_group2 > dl:nth-of-type(4) > dd', 'PBR'),
                ('div.corp_group2 > dl:nth-of-type(5) > dd', 'DivRate') # 배당수익률
            ]
            for selector, key in summary_mapping:
                self._extract_numeric_value(soup, selector, data, key)

            # --- Financial Highlight 테이블에서 나머지 정보 스크레이핑 ---
            highlight_table = soup.select_one('#highlight_D_Y')
            if highlight_table:
                # Mapping for table data
                table_mapping = {
                    'EPS': {'row_name': 'EPS', 'col_index': 5},
                    'ROE': {'row_name': 'ROE', 'col_index': 5},
                    'BPS': {'row_name': 'BPS', 'col_index': -1}, # last column
                }
                
                rows = highlight_table.select('tbody tr')
                for row in rows:
                    th = row.select_one('th.clf')
                    if not th: continue
                    
                    item_name = th.text.strip()
                    
                    for key, mapping_info in table_mapping.items():
                        # 정확한 문자열 일치를 확인하여 'BPS'에서 'EPS'가 일치하는 것을 방지
                        if item_name == mapping_info['row_name']:
                            tds = row.select('td')
                            # 필요한 td 셀이 존재하는지 확인
                            if len(tds) > abs(mapping_info['col_index']):
                                value_str = tds[mapping_info['col_index']].text.strip().replace(',', '')
                                if value_str and value_str not in ('-', 'N/A'):
                                    data[key] = float(value_str)
                                else:
                                    # 값이 없더라도 키는 존재하도록 보장
                                    if key not in data: data[key] = None
                            break # 일치하는 항목을 찾았으므로 다음 행으로 이동
            else:
                logging.warning(f"FnGuide에서 '{code}' 종목의 Financial Highlight 테이블을 찾을 수 없습니다.")

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
        """FinanceDataReader를 사용하여 KRX 전체 종목 정보를 가져와 DB에 저장하는 메서드"""
        try:
            logging.info("Fetching company list from KRX...")
            # KRX 전체 종목 리스트 가져오기 (재무 정보 포함)
            stocks = fdr.StockListing('KRX')

            # 시가총액(Marcap)을 기준으로 내림차순 정렬
            stocks = stocks.sort_values(by='Marcap', ascending=False)

            # DB에 저장된 모든 종목 정보를 dictionary 형태로 가져옵니다.
            # limit이 지정된 경우, 해당 종목들만 DB에서 가져와 비교 대상으로 한정합니다.
            cursor = self.db_access.connection.cursor(dictionary=True)
            if limit:
                limited_codes = tuple(stocks['Code'].tolist())
                placeholders = ', '.join(['%s'] * len(limited_codes))
                cursor.execute(f"SELECT * FROM companies WHERE code IN ({placeholders})", limited_codes)
            else:
                cursor.execute("SELECT * FROM companies")
            
            existing_stocks = {row['code']: row for row in cursor.fetchall()}    
            cursor.close()
            
            logging.info(f"DB에 존재하는 종목 수: {len(existing_stocks)}")

            # DataFrame의 컬럼명과 DB의 컬럼명을 매핑합니다.
            field_map = {
                'Name': 'name', 'Market': 'market', 'Marcap': 'marcap', 'Stocks': 'stocks', 'PBR': 'pbr', 'PER': 'per', 'EPS': 'eps', 'ROE': 'roe',
                'DivRate': 'div_yield', 'BPS': 'bps'
            }

            qualified_rows = []

            for _, row in stocks.iterrows():
                # 상세 재무 정보 스크레이핑
                fnguide_financials = self.get_financial_data_from_fnguide(row.get('Code'))
                
                # FDR 데이터와 스크래핑 데이터 병합
                # FDR에 값이 있고 네이버에 없으면 FDR 값 사용, 둘 다 있으면 네이버 값 사용
                if fnguide_financials:
                    for key, fnguide_val in fnguide_financials.items():
                        if fnguide_val is not None:
                            row[key] = fnguide_val
                
                # 조건 필터링: PER > 0 이고, EPS >= 0 인 종목만 선택
                per_value = row.get('PER')
                eps_value = row.get('EPS')

                if per_value is not None and pd.notna(per_value) and per_value > 0 and eps_value is not None and pd.notna(eps_value) and eps_value >= 0:
                    qualified_rows.append(row)
                
                # 목표 개수(limit)를 채우면 중단
                if limit and len(qualified_rows) >= limit:
                    logging.info(f"조건을 만족하는 {limit}개의 종목을 찾았습니다. 목록 수집을 중단합니다.")
                    break
            
            # --- 필터링된 종목들로 DB 작업 준비 ---
            stocks_to_upsert = []
            logging.info(f"총 {len(qualified_rows)}개의 조건 만족 종목으로 DB 작업을 시작합니다.")

            for row in qualified_rows:
                # FDR 데이터와 fnguide 스크래핑 데이터 병합
                # fnguide 데이터가 있으면 해당 값으로 업데이트
                fnguide_financials = self.get_financial_data_from_fnguide(row.get('Code'))
                if fnguide_financials:
                    for key, fnguide_val in fnguide_financials.items():
                        if fnguide_val is not None:
                            row[key] = fnguide_val
                
                # DB에 저장할 파라미터 튜플 생성
                params = (
                    row.get('Code'), row.get('Name'), row.get('Market'),
                    row.get('Marcap'), row.get('Stocks'), row.get('PBR'),
                    row.get('PER'), row.get('EPS'), row.get('ROE'),
                    row.get('DivRate'), row.get('BPS')
                )
                stocks_to_upsert.append(params)

            if stocks_to_upsert:
                logging.info(f"총 {len(stocks_to_upsert)}개 종목을 데이터베이스에 저장 또는 업데이트합니다.")
                
                # INSERT ... ON DUPLICATE KEY UPDATE 쿼리
                query = """
                INSERT INTO companies (code, name, market, marcap, stocks, pbr, per, eps, roe, div_yield, bps) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    market = VALUES(market),
                    marcap = VALUES(marcap),
                    stocks = VALUES(stocks),
                    pbr = VALUES(pbr),
                    per = VALUES(per),
                    eps = VALUES(eps),
                    roe = VALUES(roe),
                    div_yield = VALUES(div_yield),
                    bps = VALUES(bps);
                """
                self.db_access.execute_many_query(query, stocks_to_upsert)
                logging.info(f"성공적으로 {len(stocks_to_upsert)}개 종목 정보를 저장/업데이트했습니다.")
            else:
                logging.info("저장하거나 업데이트할 종목이 없습니다.")

        except Exception as e:
            logging.error(f"Error saving companies from fdr: {e}")

if __name__ == "__main__":

    with get_db_connection() as db_access:
        logging.info("CompanyManager 스크립트 시작: %s", time.ctime())

        company_manager = CompanyManager(db_access)
        if company_manager.create_companies_table():
            logging.info("종목 정보 업데이트를 시작합니다.")
            
            # .env 파일에서 COMPANY_COUNT 가져오기
            company_count_str = os.getenv('STOCK_COUNT')
            limit = int(company_count_str) if company_count_str else None
            company_manager.save_companies_from_fdr(limit=limit)

        logging.info("CompanyManager 스크립트 종료: %s", time.ctime())