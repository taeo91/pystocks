import pandas as pd
from DBAccessManager import DBAccessManager
import logging
import os
from dotenv import load_dotenv
import datetime
import re
import FinanceDataReader as fdr
from ETFManager import ETFManager
from openpyxl import load_workbook
from openpyxl.styles import Font

class PortfolioManager:
    def __init__(self, db_manager, etf_manager):
        self.db_manager = db_manager
        self.etf_manager = etf_manager

    # --- Private Helper Methods ---

    def _get_market_lookup_tables(self):
        """Fetches and consolidates market and name lookup tables from KOSPI, KOSDAQ, and ETF listings."""
        name_lookup, market_lookup = {}, {}
        sources = {'KOSPI': 'KOSPI', 'KOSDAQ': 'KOSDAQ', 'ETF': 'ETF'}
        
        for name, source in sources.items():
            try:
                logging.info(f"Fetching {name} listings...")
                listing = fdr.StockListing(source)
                listing['Symbol'] = listing['Symbol'].astype(str)
                name_lookup.update(listing.set_index('Symbol')['Name'].to_dict())
                if source != 'ETF':
                    market_lookup.update(listing.set_index('Symbol')['Market'].to_dict())
                else:
                    for ticker in listing['Symbol']:
                        market_lookup[ticker] = 'ETF'
            except Exception as e:
                logging.warning(f"Could not fetch {name} listings: {e}")
        return name_lookup, market_lookup

    def _get_or_create_company_ids(self, tickers, name_lookup, market_lookup):
        """Ensures all tickers exist as companies in the DB and returns a {ticker: company_id} map."""
        if not tickers:
            return {}

        placeholders = ', '.join(['%s'] * len(tickers))
        query = f"SELECT code FROM companies WHERE code IN ({placeholders})"
        existing_tickers = {row[0] for row in self.db_manager.fetch_all(query, tickers)}
        new_tickers = [t for t in tickers if t not in existing_tickers]

        if new_tickers:
            logging.info(f"Found {len(new_tickers)} new tickers to insert.")
            new_company_data = [
                (ticker, name_lookup.get(ticker, ticker), market_lookup.get(ticker, 'UNKNOWN'))
                for ticker in new_tickers
            ]
            insert_query = "INSERT INTO companies (code, name, market) VALUES (%s, %s, %s)"
            self.db_manager.execute_many_query(insert_query, new_company_data)
            for ticker in new_tickers:
                if market_lookup.get(ticker) == 'ETF':
                    self.etf_manager.add_etf(ticker, name_lookup.get(ticker, ticker))

        query = f"SELECT code, id FROM companies WHERE code IN ({placeholders})"
        return {code: id for code, id in self.db_manager.fetch_all(query, tickers)}

    def _create_table_from_dataframe(self, table_name, df, add_auto_increment_id=True):
        """Helper to generate and execute a CREATE TABLE query from a DataFrame."""
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ("
        if add_auto_increment_id:
            create_table_query += "`id` INT AUTO_INCREMENT PRIMARY KEY, "

        for column, dtype in df.dtypes.items():
            mysql_type = self.get_mysql_data_type(dtype)
            if column == 'ticker':
                mysql_type = 'VARCHAR(255)'
            create_table_query += f"`{column}` {mysql_type}, "
        
        create_table_query = create_table_query.rstrip(", ") + ")"
        self.db_manager.execute_query(create_table_query)
        logging.info(f"Table '{table_name}' created or already exists.")

    def _update_revision_portfolio(self, workbook, latest_db_date, tickers, placeholders):
        """Updates the 'Portfolio' sheet for revision files."""
        sheet_name = 'Portfolio'
        try:
            sheet = workbook[sheet_name]
            header = [cell.value for cell in sheet[3]] # Header on 3rd row
            code_col_idx = header.index('코드') + 1
            price_col_idx = header.index('현재가') + 1

            query = f"SELECT c.code, p.close_price FROM prices p JOIN companies c ON p.company_id = c.id WHERE p.trade_date = %s AND c.code IN ({placeholders})"
            prices_for_date = {code: price for code, price in self.db_manager.fetch_all(query, [latest_db_date] + tickers)}

            blue_font = Font(color="0000FF")
            for row_idx in range(4, sheet.max_row + 1): # Data starts from 4th row
                ticker = sheet.cell(row=row_idx, column=code_col_idx).value
                if ticker in prices_for_date:
                    price_cell = sheet.cell(row=row_idx, column=price_col_idx)
                    price_cell.value = prices_for_date[ticker]
                    price_cell.font = blue_font
        except (KeyError, ValueError, Exception) as e:
            logging.error(f"Failed to update '{sheet_name}' sheet: {e}")

    # --- Public Methods ---

    def get_mysql_data_type(self, dtype):
        if "int" in str(dtype): return "INT"
        if "float" in str(dtype): return "FLOAT"
        if "datetime" in str(dtype): return "DATETIME"
        return "VARCHAR(255)"

    def get_tickers_from_excel(self, file_path):
        """Extracts tickers from the 'Portfolio' sheet of the given Excel file."""
        try:
            sheet_name, header = 'Portfolio', 2
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header, dtype={'코드': str})
            tickers = df['코드'].dropna().apply(lambda x: str(x).zfill(6)).tolist()
            logging.info(f"Extracted {len(tickers)} tickers from file '{file_path}'.")
            return tickers
        except Exception as e:
            logging.error(f"Error reading tickers from excel file {file_path}: {e}")
            return []

    def fetch_and_save_etf_prices(self, tickers, start_date=None):
        if not start_date:
            start_date = (datetime.date.today() - datetime.timedelta(days=365)).strftime('%Y-%m-%d')

        name_lookup, market_lookup = self._get_market_lookup_tables()
        company_id_map = self._get_or_create_company_ids(tickers, name_lookup, market_lookup)
        
        company_ids = list(company_id_map.values())
        last_trade_dates = {}
        if company_ids:
            placeholders = ', '.join(['%s'] * len(company_ids))
            query = f"SELECT company_id, MAX(trade_date) FROM prices WHERE company_id IN ({placeholders}) GROUP BY company_id"
            last_dates_raw = self.db_manager.fetch_all(query, company_ids)
            last_trade_dates = {cid: date for cid, date in last_dates_raw}

        for ticker in tickers:
            company_id = company_id_map.get(ticker)
            if not company_id:
                logging.warning(f"Could not process ticker {ticker}, no company_id found. Skipping.")
                continue

            last_date = last_trade_dates.get(company_id)
            fetch_start_date = (last_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d') if last_date else start_date
            
            logging.info(f"[{ticker}] Fetching price data from {fetch_start_date}")
            try:
                df = fdr.DataReader(ticker, start=fetch_start_date)
                if df.empty:
                    logging.info(f"[{ticker}] No new price data found.")
                    continue

                data_to_insert = [(company_id, date.strftime('%Y-%m-%d'), row['Open'], row['High'], row['Low'], row['Close'], row['Volume']) for date, row in df.iterrows()]
                if data_to_insert:
                    price_query = "INSERT INTO prices (company_id, trade_date, open_price, high_price, low_price, close_price, volume) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE open_price=VALUES(open_price), high_price=VALUES(high_price), low_price=VALUES(low_price), close_price=VALUES(close_price), volume=VALUES(volume)"
                    self.db_manager.execute_many_query(price_query, data_to_insert)
                    logging.info(f"[{ticker}] Inserted/updated {len(data_to_insert)} price records.")
            except Exception as e:
                logging.error(f"Error fetching or saving price data for {ticker}: {e}")

    def import_portfolio_from_excel(self, file_path, table_name):
        try:
            df = pd.read_excel(file_path)
            logging.info(f"Successfully read Excel file: {file_path}")
        except FileNotFoundError:
            logging.error(f"Error: The file '{file_path}' was not found.")
            return

        df.columns = [col.strip().replace(" ", "_").replace("-", "_").replace("(", "").replace(")", "") for col in df.columns]
        self._create_table_from_dataframe(table_name, df, add_auto_increment_id=False)

        insert_query = f"INSERT INTO {table_name} ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(df.columns))})"
        df = df.where(pd.notna(df), None)
        data_to_insert = [tuple(row) for row in df.to_numpy()]
        self.db_manager.execute_many_query(insert_query, data_to_insert)
        logging.info(f"Data inserted into '{table_name}'.")

    def import_settings_from_excel(self, file_path, table_name):
        sheet_name = '설정'
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            settings_df = pd.DataFrame({
                '날짜': [df.iloc[2, 1]], 'CMA순자산평가금': [df.iloc[4, 1]],
                '추정현금': [df.iloc[5, 1]], '현금목표비중': [df.iloc[7, 1]]
            })
        except (FileNotFoundError, IndexError, Exception) as e:
            logging.error(f"Error reading or processing sheet '{sheet_name}': {e}")
            return
        
        settings_df.columns = [col.strip().replace(" ", "_") for col in settings_df.columns]
        self._create_table_from_dataframe(table_name, settings_df)

        self.db_manager.execute_query(f"TRUNCATE TABLE {table_name}")
        insert_query = f"INSERT INTO {table_name} ({', '.join([f'`{col}`' for col in settings_df.columns])}) VALUES ({', '.join(['%s'] * len(settings_df.columns))})"
        data_to_insert = [tuple(row) for row in settings_df.to_numpy()]
        self.db_manager.execute_many_query(insert_query, data_to_insert)
        logging.info(f"Data inserted into '{table_name}'.")

    def import_holdings_from_excel(self, file_path, table_name):
        """Imports holdings data from the 'Portfolio' sheet into a MySQL database."""
        sheet_name, header, cols_map = 'Portfolio', 2, {
            '코드': 'ticker', '종목명': 'stock_name', '보유수량': 'quantity',
            '매입단가': 'avg_price', '매입금액': 'purchase_amount', 'MDD': 'mdd_percent'
        }

        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header, dtype={'코드': str})
            holdings_df = df[list(cols_map.keys())].rename(columns=cols_map)
        except (FileNotFoundError, KeyError, Exception) as e:
            logging.error(f"Error reading or processing sheet '{sheet_name}': {e}")
            return

        holdings_df = holdings_df[holdings_df['stock_name'] != '합계'].dropna(subset=['stock_name'])
        if 'ticker' in holdings_df.columns and holdings_df['ticker'].notna().any():
            holdings_df['ticker'] = holdings_df['ticker'].dropna().apply(lambda x: str(x).zfill(6))

        ticker_list = holdings_df['ticker'].dropna().tolist()
        if ticker_list:
            placeholders = ', '.join(['%s'] * len(ticker_list))
            query = f"""
                SELECT c.code, p.close_price FROM prices p
                JOIN (
                    SELECT company_id, MAX(trade_date) as max_date FROM prices
                    WHERE company_id IN (SELECT id FROM companies WHERE code IN ({placeholders}))
                    GROUP BY company_id
                ) latest ON p.company_id = latest.company_id AND p.trade_date = latest.max_date
                JOIN companies c ON p.company_id = c.id
            """
            price_map = {code: price for code, price in self.db_manager.fetch_all(query, ticker_list)}
            holdings_df['close_price'] = holdings_df['ticker'].map(price_map)
        else:
            holdings_df['close_price'] = None

        self._create_table_from_dataframe(table_name, holdings_df)
        self.db_manager.execute_query(f"TRUNCATE TABLE {table_name}")

        insert_query = f"INSERT INTO {table_name} ({', '.join([f'`{col}`' for col in holdings_df.columns])}) VALUES ({', '.join(['%s'] * len(holdings_df.columns))})"
        holdings_df = holdings_df.where(pd.notna(holdings_df), None)
        data_to_insert = [tuple(row) for row in holdings_df.to_numpy()]
        self.db_manager.execute_many_query(insert_query, data_to_insert)
        logging.info(f"Data inserted into '{table_name}'.")

    def update_portfolio_excel_with_prices(self, file_path):
        """Updates the 'Portfolio' sheet with the latest stock prices from the database."""
        try:
            logging.info("--- Starting Excel update process ---")
            workbook = load_workbook(file_path)
            all_tickers = self.get_tickers_from_excel(file_path)
            if not all_tickers:
                logging.warning("No tickers found. Aborting update.")
                return

            placeholders = ', '.join(['%s'] * len(all_tickers))
            query = f"SELECT MAX(p.trade_date) FROM prices p JOIN companies c ON p.company_id = c.id WHERE c.code IN ({placeholders})"
            result = self.db_manager.fetch_one(query, all_tickers)
            latest_db_date = result[0] if result and result[0] else None

            if not latest_db_date:
                logging.warning("No price data in DB for any tickers. Aborting.")
                return
                
            try:
                settings_sheet = workbook['설정']
                cell_to_update = settings_sheet['B3']
                cell_to_update.value = latest_db_date
                cell_to_update.font = Font(color="0000FF")
                logging.info(f"Updated '설정' sheet with latest date: {latest_db_date}")
            except (KeyError, Exception) as e:
                logging.warning(f"Could not update '설정' sheet: {e}")

            self._update_revision_portfolio(workbook, latest_db_date, all_tickers, placeholders)

            workbook.save(file_path)
            logging.info("--- Excel file update process finished ---")
        except FileNotFoundError:
            logging.error(f"Error: File '{file_path}' not found.")
        except Exception as e:
            logging.error(f"An error occurred while updating excel: {e}", exc_info=True)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    load_dotenv()
    
    db_manager = DBAccessManager(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )
    
    portfolio_file_path = os.getenv("PORTFOLIO_EXCEL_FILE")
    if not portfolio_file_path:
        logging.error("PORTFOLIO_EXCEL_FILE environment variable not set.")
    else:
        db_manager.connect_to_mysql()

        if db_manager.connection:
            etf_manager = ETFManager(db_manager)
            portfolio_manager = PortfolioManager(db_manager, etf_manager)
            
            logging.info("Step 1: Reading tickers from Excel file...")
            tickers = portfolio_manager.get_tickers_from_excel(portfolio_file_path)

            if tickers:
                logging.info("Step 2: Fetching latest prices and saving to DB...")
                portfolio_manager.fetch_and_save_etf_prices(tickers)

                logging.info("Step 3: Updating Excel file with latest prices from DB...")
                portfolio_manager.update_portfolio_excel_with_prices(portfolio_file_path)
            
            db_manager.close_connection()
