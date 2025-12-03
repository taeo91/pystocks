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
            # FnGuide는 종목코드 앞에 'A'를 붙여야 합니다.
            url = f"http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&gicode=A{code}"
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers, timeout=5)
            # 일부 우선주 종목은 페이지가 없을 수 있음. 이 경우 보통주로 재시도.
            if "Snapshot 일부 종목에 한해" in response.text and not is_retry:
                logging.warning(f"FnGuide에서 '{code}'(우선주) 페이지를 찾을 수 없어 보통주로 재시도합니다.")
                common_stock_code = code[:-1] + '0'
                return self.get_financial_data_from_fnguide(common_stock_code, is_retry=True)

            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Financial Highlight 테이블 찾기
            highlight_table = soup.select_one('#highlight_D_Y')
            if not highlight_table:
                # 우선주가 보통주 페이지로 리다이렉트 되었지만 테이블이 없는 경우, 보통주로 재시도
                if code.endswith('5') and not is_retry: # '5'로 끝나는 우선주
                    logging.warning(f"FnGuide에서 '{code}'(우선주)의 테이블을 찾을 수 없어 보통주로 재시도합니다.")
                    common_stock_code = code[:-1] + '0'
                    return self.get_financial_data_from_fnguide(common_stock_code, is_retry=True)

                logging.warning(f"FnGuide에서 '{code}' 종목의 Financial Highlight 테이블을 찾을 수 없습니다.")
                return {}

            data = {}
            # 테이블의 각 행(tr)을 순회하며 데이터 추출
            rows = highlight_table.select('tbody tr')
            for row in rows:
                th = row.select_one('th.clf')
                # '최근결산'에 해당하는 첫 번째 td 값만 가져옴
                tds = row.select('td')
                
                if not th or not tds:
                    continue

                item = th.text.strip()
                
                # ROE, EPS, PBR은 6번째 td의 값을, 나머지는 마지막 td의 값을 사용
                target_td_index = -1 # 기본값: 마지막 td
                if item in ('ROE', 'EPS', 'PBR'):
                    if len(tds) > 5: # 6번째 td가 존재하는지 확인
                        target_td_index = 5
                
                if target_td_index == -1 and not tds: continue # td가 없으면 건너뛰기

                value_str = tds[target_td_index].text.strip().replace(',', '')
                
                if not value_str or value_str == '-' or value_str == 'N/A':
                    value = None
                else:
                    value = float(value_str)

                if 'PER' in item and 'EPS' not in item: data['PER'] = value
                elif 'PBR' in item: data['PBR'] = value
                elif 'EPS' in item: data['EPS'] = value
                elif 'ROE' in item: data['ROE'] = value
                elif 'BPS' in item: data['BPS'] = value
                elif '배당수익률' in item: data['DivRate'] = value

            return data
        except Exception as e:
            logging.warning(f"FnGuide에서 '{code}' 종목의 재무 정보 스크레이핑 실패: {e}")
            return {}

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
            new_stocks_to_insert = []
            updates_to_execute = []

            logging.info(f"총 {len(qualified_rows)}개의 조건 만족 종목으로 DB 작업을 시작합니다.")

            for row in qualified_rows:
                code = row.get('Code')
                # DB에서 가져온 기존 종목 목록에 현재 코드가 있는지 확인
                db_stock = existing_stocks.get(code)

                if not db_stock: # 신규 종목일 경우
                    params = tuple(row.get(df_col) for df_col in ['Code'] + list(field_map.keys()))
                    new_stocks_to_insert.append(params)
                else: # 기존 종목일 경우, 변경된 값 확인
                    update_fields = []
                    update_values = []

                    for df_col, db_col in field_map.items():
                        fdr_val = row.get(df_col)
                        db_val = db_stock.get(db_col)

                        # pandas의 NaN/NaT는 None으로 처리
                        if pd.isna(fdr_val):
                            fdr_val = None
                        
                        # DB의 Decimal 타입을 float으로 변환하여 비교
                        if isinstance(db_val, Decimal):
                            db_val = float(db_val)

                        # 값이 다른 경우 업데이트 목록에 추가
                        if fdr_val != db_val:
                            update_fields.append(f"{db_col} = %s")
                            update_values.append(fdr_val)
                    
                    if update_fields:
                        update_values.append(code) # WHERE 절에 들어갈 종목 코드
                        updates_to_execute.append({
                            "fields": ", ".join(update_fields),
                            "values": tuple(update_values)
                        })

            if new_stocks_to_insert:
                logging.info(f"총 {len(new_stocks_to_insert)}개의 새로운 종목을 데이터베이스에 저장합니다.")
                query = """
                INSERT INTO companies (code, name, market, marcap, stocks, pbr, per, eps, roe, div_yield, bps) 
                VALUES ({})
                """
                placeholders = ", ".join(["%s"] * 11)
                insert_query = query.format(placeholders)
                
                # executemany를 사용하도록 DBAccessManager 수정 또는 반복 실행
                # 현재 DBAccessManager는 executemany를 지원하지 않으므로 반복 실행
                for params in new_stocks_to_insert: 
                    self.db_access.execute_query(insert_query, params)
                logging.info(f"성공적으로 {len(new_stocks_to_insert)}개의 신규 종목 정보를 저장했습니다.")
            else:
                logging.info("새로 추가할 종목이 없습니다.")

            if updates_to_execute:
                logging.info(f"총 {len(updates_to_execute)}개 종목의 정보를 업데이트합니다.")
                for update in updates_to_execute:
                    query = f"UPDATE companies SET {update['fields']} WHERE code = %s"
                    self.db_access.execute_query(query, update['values'])
                logging.info("성공적으로 종목 정보를 업데이트했습니다.")
            else:
                logging.info("업데이트할 종목 정보가 없습니다.")

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