import pandas as pd
from DBAccessManager import DBAccessManager
import logging
import os
from dotenv import load_dotenv
import datetime
import re
import FinanceDataReader as fdr
import requests
from bs4 import BeautifulSoup
from ETFManager import ETFManager
from openpyxl import load_workbook
from openpyxl.styles import Font

class PortfolioManager:
    def __init__(self, db_manager, etf_manager):
        self.db_manager = db_manager
        self.etf_manager = etf_manager

    def get_mysql_data_type(self, dtype):
        if "int" in str(dtype):
            return "INT"
        elif "float" in str(dtype):
            return "FLOAT"
        elif "datetime" in str(dtype):
            return "DATETIME"
        else:
            return "VARCHAR(255)"

    def get_tickers_from_excel(self, file_path):
        if re.search(r'r\d+', file_path):
            sheet_name = 'Portfolio'
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=2, dtype={'코드': str}) # header is the 3rd row
                tickers = df['코드'].dropna().apply(lambda x: x.zfill(6)).tolist()
                logging.info(f"Extracted tickers from revision file '{sheet_name}': {tickers}")
                return tickers
            except Exception as e:
                logging.error(f"Error reading tickers from revision excel: {e}")
                return []
        else:
            sheet_name = '일일종가'
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)
                tickers = df.columns[1:-1].astype(str).tolist() # Exclude '날짜' and '메모'
                logging.info(f"Extracted tickers from '{sheet_name}': {tickers}")
                return tickers
            except Exception as e:
                logging.error(f"Error reading tickers from excel: {e}")
                return []

    def fetch_and_save_etf_prices(self, tickers, start_date=None):
        if not start_date:
            start_date = (datetime.date.today() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')

        name_lookup = {}
        market_lookup = {}

        try:
            logging.info("Fetching KOSPI/KOSDAQ listings...")
            kospi = fdr.StockListing('KOSPI')
            kosdaq = fdr.StockListing('KOSDAQ')
            listings = pd.concat([kospi, kosdaq], ignore_index=True)
            listings['Symbol'] = listings['Symbol'].astype(str)
            name_lookup.update(listings.set_index('Symbol')['Name'].to_dict())
            for _, row in listings.iterrows():
                market_lookup[row['Symbol']] = row['Market']
        except Exception as e:
            logging.warning(f"Could not fetch KOSPI/KOSDAQ listings: {e}")

        try:
            logging.info("Fetching ETF listing...")
            etf_listing = fdr.StockListing('ETF')
            etf_listing['Symbol'] = etf_listing['Symbol'].astype(str)
            name_lookup.update(etf_listing.set_index('Symbol')['Name'].to_dict())
            for ticker in etf_listing['Symbol']:
                market_lookup[ticker] = 'ETF'
        except Exception as e:
            logging.warning(f"Could not fetch ETF listings: {e}")


        for ticker in tickers:
            # 1. Ensure company exists in `companies` table
            company_id = self.db_manager.fetch_one("SELECT id FROM companies WHERE code = %s", (ticker,))
            if company_id:
                company_id = company_id[0]
            else:
                # Insert new company
                stock_name = name_lookup.get(ticker, ticker) # Use ticker as name if not found
                market = market_lookup.get(ticker, 'UNKNOWN')
                
                insert_query = "INSERT INTO companies (code, name, market) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE name=VALUES(name), market=VALUES(market)"
                self.db_manager.execute_query(insert_query, (ticker, stock_name, market))
                company_id_result = self.db_manager.fetch_one("SELECT id FROM companies WHERE code = %s", (ticker,))
                if company_id_result:
                    company_id = company_id_result[0]
                    logging.info(f"Inserted new company: {ticker} - {stock_name}")
                else:
                    logging.error(f"Failed to insert or retrieve company_id for {ticker}")
                    continue

                if market == 'ETF':
                    self.etf_manager.add_etf(ticker, stock_name)


            # 2. Fetch prices
            last_date_result = self.db_manager.fetch_one("SELECT MAX(trade_date) FROM prices WHERE company_id = %s", (company_id,))
            
            fetch_start_date = start_date
            if last_date_result and last_date_result[0]:
                fetch_start_date = (last_date_result[0] + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
            
            logging.info(f"[{ticker}] Fetching price data from {fetch_start_date}")

            try:
                df = fdr.DataReader(ticker, start=fetch_start_date)
                if df.empty:
                    logging.info(f"[{ticker}] No new price data found.")
                    continue

                data_to_insert = [
                    (company_id, trade_date.strftime('%Y-%m-%d'), row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])
                    for trade_date, row in df.iterrows()
                ]

                if data_to_insert:
                    price_query = """
                    INSERT INTO prices (company_id, trade_date, open_price, high_price, low_price, close_price, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        open_price=VALUES(open_price), high_price=VALUES(high_price), low_price=VALUES(low_price), 
                        close_price=VALUES(close_price), volume=VALUES(volume)
                    """
                    self.db_manager.execute_many_query(price_query, data_to_insert)
                    logging.info(f"[{ticker}] Inserted/updated {len(data_to_insert)} price records.")

            except Exception as e:
                logging.error(f"Error fetching or saving price data for {ticker}: {e}")

    def import_portfolio_from_excel(self, file_path, table_name):
        """
        Imports portfolio data from an Excel file into a MySQL database.
        """
        try:
            df = pd.read_excel(file_path)
            logging.info(f"Successfully read Excel file: {file_path}")
        except FileNotFoundError:
            logging.error(f"Error: The file '{file_path}' was not found.")
            return

        # Sanitize column names for SQL
        df.columns = [col.strip().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "") for col in df.columns]

        # Create table if not exists
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        for column, dtype in df.dtypes.items():
            mysql_type = self.get_mysql_data_type(dtype)
            create_table_query += f"`{column}` {mysql_type}, "
        create_table_query = create_table_query.rstrip(", ") + ")"
        
        self.db_manager.execute_query(create_table_query)
        logging.info(f"Table '{table_name}' created or already exists.")

        # Insert data
        insert_query = f"INSERT INTO {table_name} ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(df.columns))})"
        
        # Convert DataFrame to list of tuples, handling NaT and NaN
        # Replace NaN with None for SQL compatibility
        df = df.where(pd.notna(df), None)
        data_to_insert = [tuple(row) for row in df.to_numpy()]
        
        self.db_manager.execute_many_query(insert_query, data_to_insert)
        logging.info(f"Data inserted into '{table_name}'.")


    def import_settings_from_excel(self, file_path, table_name):
        """
        Imports settings data from a specific sheet in an Excel file into a MySQL database.
        """
        sheet_name = '설정'
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            logging.info(f"Successfully read sheet '{sheet_name}' from Excel file: {file_path}")
        except FileNotFoundError:
            logging.error(f"Error: The file '{file_path}' was not found.")
            return
        except Exception as e:
            logging.error(f"Error reading sheet '{sheet_name}': {e}")
            return

        # Extract data from specific cells
        try:
            data = {
                '날짜': [df.iloc[2, 1]],
                'CMA순자산평가금': [df.iloc[4, 1]],
                '추정현금': [df.iloc[5, 1]],
                '현금목표비중': [df.iloc[7, 1]]
            }
            settings_df = pd.DataFrame(data)
        except IndexError:
            logging.error("Could not extract settings data. The sheet structure might be different than expected.")
            return
        
        # Sanitize column names
        settings_df.columns = [col.strip().replace(" ", "_") for col in settings_df.columns]

        # Create table
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        create_table_query += "`id` INT AUTO_INCREMENT PRIMARY KEY, "
        for column, dtype in settings_df.dtypes.items():
            mysql_type = self.get_mysql_data_type(dtype)
            create_table_query += f"`{column}` {mysql_type}, "
        create_table_query = create_table_query.rstrip(", ") + ")"
        
        self.db_manager.execute_query(create_table_query)
        logging.info(f"Table '{table_name}' created or already exists.")

        # Insert data
        # Clear the table before inserting new data
        self.db_manager.execute_query(f"TRUNCATE TABLE {table_name}")
        logging.info(f"Table '{table_name}' truncated.")
        
        insert_query = f"INSERT INTO {table_name} ({', '.join([f'`{col}`' for col in settings_df.columns])}) VALUES ({', '.join(['%s'] * len(settings_df.columns))})"
        
        data_to_insert = [tuple(row) for row in settings_df.to_numpy()]
        
        self.db_manager.execute_many_query(insert_query, data_to_insert)
        logging.info(f"Data inserted into '{table_name}'.")


    def import_holdings_from_excel(self, file_path, table_name):
        """
        Imports holdings data from a specific sheet in an Excel file into a MySQL database.
        """
        sheet_name = '보유현황'
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=0) # header is the first row
            logging.info(f"Successfully read sheet '{sheet_name}' from Excel file: {file_path}")
        except FileNotFoundError:
            logging.error(f"Error: The file '{file_path}' was not found.")
            return
        except Exception as e:
            logging.error(f"Error reading sheet '{sheet_name}': {e}")
            return

        # Select and rename columns
        required_columns = {
            '코드': 'ticker',
            '종목': 'stock_name',
            '보유량': 'quantity',
            '평균단가': 'avg_price',
            '투자금': 'purchase_amount',
            '연중최대하락폭': 'mdd_percent'
        }
        
        try:
            holdings_df = df[list(required_columns.keys())].rename(columns=required_columns)
        except KeyError as e:
            logging.error(f"A required column is missing from the excel sheet: {e}")
            return

        # Remove the summary row and any rows with null stock_name
        holdings_df = holdings_df[holdings_df['stock_name'] != '합계']
        holdings_df = holdings_df.dropna(subset=['stock_name'])

        # Fetch close prices from the database
        prices = []
        for ticker in holdings_df['ticker']:
            query = """
                SELECT p.close_price 
                FROM prices p
                JOIN companies c ON p.company_id = c.id
                WHERE c.code = %s
                ORDER BY p.trade_date DESC
                LIMIT 1
            """
            result = self.db_manager.fetch_one(query, (ticker,))
            if result:
                prices.append(result[0])
            else:
                prices.append(None)
                logging.warning(f"Could not find price for ticker {ticker} in the database.")
        
        holdings_df['close_price'] = prices


        # Create table
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        create_table_query += "`id` INT AUTO_INCREMENT PRIMARY KEY, "
        for column, dtype in holdings_df.dtypes.items():
            mysql_type = self.get_mysql_data_type(dtype)
            # Make ticker a VARCHAR
            if column == 'ticker':
                mysql_type = 'VARCHAR(255)'
            create_table_query += f"`{column}` {mysql_type}, "
        create_table_query = create_table_query.rstrip(", ") + ")"
        
        self.db_manager.execute_query(create_table_query)
        logging.info(f"Table '{table_name}' created or already exists.")

        # Insert data
        self.db_manager.execute_query(f"TRUNCATE TABLE {table_name}")
        logging.info(f"Table '{table_name}' truncated.")

        insert_query = f"INSERT INTO {table_name} ({', '.join([f'`{col}`' for col in holdings_df.columns])}) VALUES ({', '.join(['%s'] * len(holdings_df.columns))})"
        
        # Replace NaN with None for SQL compatibility
        holdings_df = holdings_df.where(pd.notna(holdings_df), None)
        data_to_insert = [tuple(row) for row in holdings_df.to_numpy()]
        
        self.db_manager.execute_many_query(insert_query, data_to_insert)
        logging.info(f"Data inserted into '{table_name}'.")


    def update_portfolio_excel_with_prices(self, file_path):
        """
        Updates the '일일종가' sheet in the given Excel file with the latest stock prices from the database.
        If the latest price date from the DB is not in the excel, it appends a new row.
        Otherwise, it updates the row corresponding to the latest date.
        """
        sheet_name = '일일종가'
        try:
            logging.info("--- Starting Excel update process ---")
            workbook = load_workbook(file_path)
            sheet = workbook[sheet_name]
            logging.info(f"Successfully loaded sheet '{sheet_name}' from '{file_path}'")
            
            header = [cell.value for cell in sheet[1]]
            tickers = header[1:-1] # Exclude '날짜' and '메모'
            logging.info(f"Found tickers in excel: {tickers}")
            ticker_col_map = {ticker: i for i, ticker in enumerate(header) if ticker and ticker not in ['날짜', '메모']}

            # Get last date from Excel
            excel_dates_raw = [sheet.cell(row=i, column=1).value for i in range(2, sheet.max_row + 1)]
            excel_dates = sorted([d.date() if isinstance(d, datetime.datetime) else d for d in excel_dates_raw if isinstance(d, (datetime.datetime, datetime.date))])
            last_excel_date = excel_dates[-1] if excel_dates else None
            logging.info(f"Last date found in Excel: {last_excel_date}")
            
            # Get latest date from DB for these tickers
            if not tickers:
                logging.warning("No tickers found in Excel sheet.")
                return
            
            non_empty_tickers = [t for t in tickers if t]
            if not non_empty_tickers:
                logging.warning("No non-empty tickers found.")
                return

            placeholders = ', '.join(['%s'] * len(non_empty_tickers))
            query = f"SELECT MAX(p.trade_date) FROM prices p JOIN companies c ON p.company_id = c.id WHERE c.code IN ({placeholders})"
            latest_db_date_result = self.db_manager.fetch_one(query, non_empty_tickers)
            latest_db_date = latest_db_date_result[0] if latest_db_date_result and latest_db_date_result[0] else None
            logging.info(f"Latest date found in DB for these tickers: {latest_db_date}")

            if not latest_db_date:
                logging.warning("No price data found in DB for any of the tickers.")
                return

            # 설정 시트의 기준일자를 최신 DB 데이터 날짜로 업데이트하고 파란색으로 표시
            try:
                settings_sheet = workbook['설정']
                cell_to_update = settings_sheet['B3']
                cell_to_update.value = latest_db_date
                
                # 파란색 폰트 설정
                blue_font = Font(color="0000FF")
                cell_to_update.font = blue_font
                
                logging.info(f"Updated cell B3 in '설정' sheet with latest date: {latest_db_date} and set font to blue.")
            except KeyError:
                logging.warning("'설정' sheet not found, cannot update latest date.")
            except Exception as e:
                logging.error(f"An error occurred while updating the '설정' sheet: {e}")
                
            target_date = latest_db_date
            target_row_idx = -1

            if not last_excel_date or target_date > last_excel_date:
                target_row_idx = sheet.max_row + 1
                sheet.cell(row=target_row_idx, column=1, value=target_date)
                logging.info(f"Decision: Append new row for date {target_date} at row {target_row_idx}")
            elif target_date in excel_dates:
                # Find the row index for the target_date
                for i, d in enumerate(excel_dates_raw, start=2):
                    current_date = d.date() if isinstance(d, datetime.datetime) else d
                    if current_date == target_date:
                        target_row_idx = i
                        break
                logging.info(f"Decision: Update existing row {target_row_idx} for date {target_date}")
            else:
                 logging.info(f"Decision: Latest date in DB ({target_date}) is not newer than in Excel ({last_excel_date}) and not present. No update.")
                 return

            if target_row_idx == -1:
                logging.error("Could not determine row to update. Aborting.")
                return

            # Fetch all prices for the target date
            query = f"SELECT c.code, p.close_price FROM prices p JOIN companies c ON p.company_id = c.id WHERE p.trade_date = %s AND c.code IN ({placeholders})"
            params = [target_date] + non_empty_tickers
            prices_for_date_raw = self.db_manager.fetch_all(query, params)
            prices_for_date = {code: price for code, price in prices_for_date_raw}
            logging.info(f"Prices found for date {target_date}: {prices_for_date}")
            
            # Update row
            blue_font = Font(color="0000FF")
            for ticker, header_idx in ticker_col_map.items():
                col_idx = header_idx + 1
                if ticker in prices_for_date:
                    price_to_set = prices_for_date[ticker]
                    cell_to_update = sheet.cell(row=target_row_idx, column=col_idx)
                    cell_to_update.value = price_to_set
                    cell_to_update.font = blue_font
                    logging.info(f"Updating cell ({target_row_idx}, {col_idx}) for ticker {ticker} with price {price_to_set}")

            workbook.save(file_path)
            logging.info("--- Excel file update process finished ---")

        except FileNotFoundError:
            logging.error(f"Error: The file '{file_path}' was not found.")
        except KeyError:
            logging.error(f"Error: Sheet '{sheet_name}' not found in '{file_path}'.")
        except Exception as e:
            logging.error(f"An error occurred while updating the excel file: {e}", exc_info=True)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    load_dotenv()
    
    db_manager = DBAccessManager(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    
    db_manager.connect_to_mysql()

    if db_manager.connection:
        etf_manager = ETFManager(db_manager)
        portfolio_manager = PortfolioManager(db_manager, etf_manager)
        
        # First, fetch prices which also creates the company entries
        tickers = portfolio_manager.get_tickers_from_excel('reports/portfolio_r14.xlsx')
        if tickers:
            portfolio_manager.fetch_and_save_etf_prices(tickers)
        
        
        db_manager.close_connection()
