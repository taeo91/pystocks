import logging
import requests
from bs4 import BeautifulSoup
import datetime
import FinanceDataReader as fdr
import pandas as pd
import os
from AppManager import get_db_connection

class ETFManager:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def create_etf_info_table(self):
        """'etf_info' 테이블을 생성합니다."""
        query = """
        CREATE TABLE IF NOT EXISTS etf_info (
            id INT AUTO_INCREMENT PRIMARY KEY,
            code VARCHAR(20) NOT NULL UNIQUE COMMENT '종목코드',
            name VARCHAR(255) COMMENT '종목명',
            market_sum BIGINT COMMENT '시가총액(억)',
            nav DECIMAL(15, 2) COMMENT 'NAV',
            three_month_earn_rate DECIMAL(10, 2) COMMENT '3개월 수익률(%)',
            total_expense_ratio DECIMAL(10, 4) COMMENT '총보수(%)',
            dividend_yield DECIMAL(10, 2) COMMENT '분배율(%)',
            FOREIGN KEY (code) REFERENCES companies (code) ON DELETE CASCADE
        ) COMMENT 'ETF 상세 정보';
        """
        try:
            self.db_manager.execute_query(query)
            logging.info("Table 'etf_info' created or already exists.")
            return True
        except Exception as e:
            logging.error(f"Error creating 'etf_info' table: {e}")
            return False

    def update_etf_names_from_naver(self):
        """
        Fetches ETF names from Naver Finance API and updates them in the database.
        """
        logging.info("Starting to fetch ETF names from Naver Finance API.")
        
        etf_name_map = {}
        
        # URL for the API that returns ETF list as JSON
        url = "https://finance.naver.com/api/sise/etfItemList.nhn?etfType=0&targetColumn=market_sum&sortOrder=desc"
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            # Set the encoding to euc-kr and then parse the JSON
            response.encoding = 'euc-kr'
            data = response.json()
            
            if data.get('resultCode') == 'success':
                etf_list = data.get('result', {}).get('etfItemList', [])
                for etf in etf_list:
                    ticker = etf.get('itemcode')
                    name = etf.get('itemname')
                    if ticker and name:
                        etf_name_map[ticker] = name
                logging.info(f"Successfully fetched {len(etf_name_map)} ETF names from Naver API.")
            else:
                logging.error(f"Naver API returned an error: {data.get('resultCode')}")
                return

        except requests.RequestException as e:
            logging.error(f"Could not fetch data from Naver API: {e}")
            return
        except ValueError: # Catches JSON decoding errors
             logging.error("Failed to decode JSON from Naver API response.")
             return
        
        if not etf_name_map:
            logging.warning("Could not fetch any ETF names from Naver Finance API.")
            return

        # Get companies that need name updates
        query = "SELECT id, code, name FROM companies WHERE market = 'ETF' OR market = 'UNKNOWN' OR code = name"
        companies_to_update = self.db_manager.fetch_all(query)

        if not companies_to_update:
            logging.info("No companies found that need a name update.")
            return

        updated_count = 0
        for company in companies_to_update:
            company_id, code, old_name = company
            if code in etf_name_map and etf_name_map[code] != old_name:
                new_name = etf_name_map[code]
                update_query = "UPDATE companies SET name = %s, market = 'ETF' WHERE id = %s"
                self.db_manager.execute_query(update_query, (new_name, company_id))
                logging.info(f"Updated name for {code}: '{old_name}' -> '{new_name}'")
                updated_count += 1
        
        logging.info(f"ETF name update complete. Updated {updated_count} records.")

    def update_etf_details(self):
        """
        Naver Finance에서 ETF 상세 정보를 가져와 'etf_info' 테이블을 업데이트합니다.
        """
        logging.info("Updating ETF details from Naver Finance.")
        
        # 1. DB에서 ETF 목록 가져오기
        query = "SELECT code, name FROM companies WHERE market = 'ETF'"
        etfs = self.db_manager.fetch_all(query)
        
        if not etfs:
            logging.info("No ETFs found in the database to update details for.")
            return

        logging.info(f"Found {len(etfs)} ETFs to update.")

        etf_details_to_insert = []

        # 2. Naver API에서 전체 ETF 정보 가져오기 (기본 정보)
        etf_api_data = {}
        try:
            url = "https://finance.naver.com/api/sise/etfItemList.nhn?etfType=0&targetColumn=market_sum&sortOrder=desc"
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            response.encoding = 'euc-kr'
            data = response.json()
            if data.get('resultCode') == 'success':
                for item in data.get('result', {}).get('etfItemList', []):
                    etf_api_data[item['itemcode']] = {
                        'market_sum': item.get('marketSum'),
                        'nav': item.get('nav'),
                        'three_month_earn_rate': item.get('threeMonthEarnRate')
                    }
            logging.info(f"Successfully fetched basic data for {len(etf_api_data)} ETFs from API.")
        except Exception as e:
            logging.error(f"Failed to fetch ETF list from Naver API: {e}")
            # Continue without this data if API fails
        
        # 3. 각 ETF에 대해 상세 정보 스크래핑 및 데이터 취합
        for code, name in etfs:
            detail_url = f"https://finance.naver.com/item/main.naver?code={code}"
            total_expense_ratio = None
            dividend_yield = None
            
            try:
                headers = {'User-Agent': 'Mozilla/5.0'}
                res = requests.get(detail_url, headers=headers)
                res.raise_for_status()
                
                # Parsing with BeautifulSoup
                soup = BeautifulSoup(res.text, 'html.parser')

                # 총보수 (Total Expense Ratio)
                summary_info = soup.find('div', class_='summary_info')
                if summary_info:
                    # '총보수'라는 텍스트를 포함하는 th를 찾고, 그 다음 td의 값을 가져옴
                    th = summary_info.find('th', string=lambda text: text and '총보수' in text)
                    if th and th.find_next_sibling('td'):
                        ter_text = th.find_next_sibling('td').get_text(strip=True)
                        # '%' 문자를 제거하고 숫자로 변환
                        total_expense_ratio = float(ter_text.replace('%', ''))

                # 분배율 (Dividend Yield)
                chart_info = soup.find('div', class_='chart_info')
                if chart_info:
                    # '분배율' 텍스트를 가진 dt를 찾고, 그 다음 dd의 값을 가져옴
                    dt = chart_info.find('dt', string=lambda text: text and '분배율' in text)
                    if dt and dt.find_next_sibling('dd'):
                        dy_text = dt.find_next_sibling('dd').get_text(strip=True)
                        # '%' 문자를 제거하고 숫자로 변환
                        dividend_yield = float(dy_text.replace('%', ''))
                
                logging.info(f"Scraped details for {code}: TER={total_expense_ratio}, DY={dividend_yield}")

            except requests.RequestException as e:
                logging.warning(f"Could not fetch detail page for {code}: {e}")
            except Exception as e:
                logging.error(f"Error parsing detail page for {code}: {e}")
            
            # 4. DB에 저장할 데이터 준비
            api_data = etf_api_data.get(code, {})
            etf_details_to_insert.append((
                code,
                name,
                api_data.get('market_sum'),
                api_data.get('nav'),
                api_data.get('three_month_earn_rate'),
                total_expense_ratio,
                dividend_yield
            ))

        # 5. DB에 데이터 삽입/업데이트
        if etf_details_to_insert:
            query = """
            INSERT INTO etf_info (code, name, market_sum, nav, three_month_earn_rate, total_expense_ratio, dividend_yield)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                market_sum = VALUES(market_sum),
                nav = VALUES(nav),
                three_month_earn_rate = VALUES(three_month_earn_rate),
                total_expense_ratio = VALUES(total_expense_ratio),
                dividend_yield = VALUES(dividend_yield);
            """
            self.db_manager.execute_many_query(query, etf_details_to_insert)
            logging.info(f"Successfully inserted/updated details for {len(etf_details_to_insert)} ETFs.")

    def add_etf(self, code, name):
        """
        새로운 ETF를 'etf_info' 테이블에 추가합니다.
        """
        query = """
        INSERT INTO etf_info (code, name)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name);
        """
        try:
            self.db_manager.execute_query(query, (code, name))
            logging.info(f"Added/updated ETF {code} in etf_info table.")
        except Exception as e:
            logging.error(f"Error adding ETF {code} to etf_info table: {e}")

if __name__ == '__main__':
    with get_db_connection() as db_manager:
        etf_manager = ETFManager(db_manager)
        etf_manager.create_etf_info_table()
        etf_manager.update_etf_names_from_naver()
        etf_manager.update_etf_details()
