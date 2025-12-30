import logging
import pandas as pd

class IndicatorManager:
    """
    기술적 지표를 계산하고 관리하는 클래스
    """
    def __init__(self, db_access):
        """
        IndicatorManager 생성자

        Args:
            db_access (dbaccess): 데이터베이스 접근을 위한 dbaccess 객체
        """
        self.db_access = db_access

    def create_indicators_tables(self):
        """'macd', 'rsi' 테이블을 생성하는 메서드"""
        try:
            # MACD 테이블 생성
            macd_query = """
            CREATE TABLE IF NOT EXISTS macd (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                company_id INT NOT NULL,
                trade_date DATE NOT NULL,
                macd DECIMAL(20, 6),
                macd_signal DECIMAL(20, 6),
                macd_hist DECIMAL(20, 6),
                `cross` VARCHAR(10) COMMENT '골든/데드 크로스',
                FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
                UNIQUE KEY (company_id, trade_date)
            ) COMMENT 'MACD 지표';
            """
            self.db_access.execute_query(macd_query)
            logging.info("Table 'macd' created or already exists.")

            # RSI 테이블 생성
            rsi_query = """
            CREATE TABLE IF NOT EXISTS rsi (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                company_id INT NOT NULL,
                trade_date DATE NOT NULL,
                rsi DECIMAL(10, 6),
                `signal` VARCHAR(10) COMMENT '매수/매도 신호',
                FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
                UNIQUE KEY (company_id, trade_date)
            ) COMMENT 'RSI 지표';
            """
            self.db_access.execute_query(rsi_query)
            logging.info("Table 'rsi' created or already exists.")

            # 이동평균선 테이블 생성
            ma_query = """
            CREATE TABLE IF NOT EXISTS moving_average (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                company_id INT NOT NULL,
                trade_date DATE NOT NULL,
                ma_5 DECIMAL(20, 6),
                ma_20 DECIMAL(20, 6),
                ma_60 DECIMAL(20, 6),
                ma_120 DECIMAL(20, 6),
                cross_5_20 VARCHAR(10) COMMENT '5일/20일 크로스',
                cross_20_60 VARCHAR(10) COMMENT '20일/60일 크로스',
                cross_60_120 VARCHAR(10) COMMENT '60일/120일 크로스',
                FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE,
                UNIQUE KEY (company_id, trade_date)
            ) COMMENT '이동평균선';
            """
            self.db_access.execute_query(ma_query)
            logging.info("Table 'moving_average' created or already exists.")
            return True
        except Exception as e:
            logging.error(f"Error creating indicator tables: {e}")
            return False

    def calculate_and_save_indicators(self, company_id):
        """특정 회사의 MACD 및 RSI 지표를 계산하고 DB에 저장하는 메서드"""
        try:
            # 가격 데이터 가져오기
            query = "SELECT trade_date, close_price FROM company_prices WHERE company_id = %s ORDER BY trade_date"
            cursor = self.db_access.connection.cursor(dictionary=True)
            cursor.execute(query, (company_id,))
            prices_data = cursor.fetchall()
            cursor.close()

            if len(prices_data) < 120: # 120일 이평선 계산을 위한 최소 데이터 수
                logging.info(f"Not enough data to calculate indicators for company_id {company_id}.")
                return

            df = pd.DataFrame(prices_data)
            df = df.set_index('trade_date')
            df['close_price'] = df['close_price'].astype(float)

            # MACD 계산
            ema12 = df['close_price'].ewm(span=12, adjust=False).mean()
            ema26 = df['close_price'].ewm(span=26, adjust=False).mean()
            macd = ema12 - ema26
            macd_signal = macd.ewm(span=9, adjust=False).mean()
            macd_hist = macd - macd_signal

            # MACD 크로스 계산
            macd_hist_prev = macd_hist.shift(1)
            golden_cross = (macd_hist >= 0) & (macd_hist_prev < 0)
            dead_cross = (macd_hist <= 0) & (macd_hist_prev > 0)

            # RSI 계산 (14일 기준)
            delta = df['close_price'].diff()
            gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
            loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            
            # RSI 신호 계산
            rsi_prev = rsi.shift(1)
            buy_signal = (rsi >= 30) & (rsi_prev < 30) # 과매도 탈출
            sell_signal = (rsi <= 70) & (rsi_prev > 70) # 과매수 진입

            # 이동평균선 계산
            ma5 = df['close_price'].rolling(window=5).mean()
            ma20 = df['close_price'].rolling(window=20).mean()
            ma60 = df['close_price'].rolling(window=60).mean()
            ma120 = df['close_price'].rolling(window=120).mean()

            # 이동평균선 크로스 계산
            def get_cross(ma_short, ma_long):
                ma_short_prev = ma_short.shift(1)
                ma_long_prev = ma_long.shift(1)
                golden = (ma_short >= ma_long) & (ma_short_prev < ma_long_prev)
                dead = (ma_short <= ma_long) & (ma_short_prev > ma_long_prev)
                return golden, dead

            golden_5_20, dead_5_20 = get_cross(ma5, ma20)
            golden_20_60, dead_20_60 = get_cross(ma20, ma60)
            golden_60_120, dead_60_120 = get_cross(ma60, ma120)

            # DB에 저장
            macd_data = []
            rsi_data = []
            ma_data = []
            for trade_date in df.index:
                cross_val = 'GOLDEN' if golden_cross.get(trade_date) else ('DEAD' if dead_cross.get(trade_date) else None)
                macd_data.append((company_id, trade_date, macd.get(trade_date), macd_signal.get(trade_date), macd_hist.get(trade_date), cross_val))

                signal_val = 'BUY' if buy_signal.get(trade_date) else ('SELL' if sell_signal.get(trade_date) else None)
                rsi_data.append((company_id, trade_date, rsi.get(trade_date), signal_val))

                cross_5_20_val = 'GOLDEN' if golden_5_20.get(trade_date) else ('DEAD' if dead_5_20.get(trade_date) else None)
                cross_20_60_val = 'GOLDEN' if golden_20_60.get(trade_date) else ('DEAD' if dead_20_60.get(trade_date) else None)
                cross_60_120_val = 'GOLDEN' if golden_60_120.get(trade_date) else ('DEAD' if dead_60_120.get(trade_date) else None)
                ma_data.append((
                    company_id, trade_date, ma5.get(trade_date), ma20.get(trade_date), ma60.get(trade_date), ma120.get(trade_date),
                    cross_5_20_val, cross_20_60_val, cross_60_120_val
                ))

            # ON DUPLICATE KEY UPDATE를 사용하여 데이터 삽입/업데이트
            macd_query = """
            INSERT INTO macd (company_id, trade_date, macd, macd_signal, macd_hist, `cross`)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                macd=VALUES(macd), 
                macd_signal=VALUES(macd_signal), 
                macd_hist=VALUES(macd_hist),
                `cross`=VALUES(`cross`)
            """
            cursor = self.db_access.connection.cursor()
            # NaN 값을 None으로 변환
            macd_data_cleaned = [tuple(None if pd.isna(v) else v for v in row) for row in macd_data]
            cursor.executemany(macd_query, macd_data_cleaned)
            self.db_access.connection.commit()
            logging.info(f"Saved/Updated {cursor.rowcount} MACD records for company_id {company_id}.")
            cursor.close()

            rsi_query = """
            INSERT INTO rsi (company_id, trade_date, rsi, `signal`)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                rsi=VALUES(rsi),
                `signal`=VALUES(`signal`)
            """
            cursor = self.db_access.connection.cursor()
            # NaN 값을 None으로 변환
            rsi_data_cleaned = [tuple(None if pd.isna(v) else v for v in row) for row in rsi_data]
            cursor.executemany(rsi_query, rsi_data_cleaned)
            self.db_access.connection.commit()
            logging.info(f"Saved/Updated {cursor.rowcount} RSI records for company_id {company_id}.")
            cursor.close()

            ma_query = """
            INSERT INTO moving_average (company_id, trade_date, ma_5, ma_20, ma_60, ma_120, cross_5_20, cross_20_60, cross_60_120)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                ma_5=VALUES(ma_5), ma_20=VALUES(ma_20), ma_60=VALUES(ma_60), ma_120=VALUES(ma_120),
                cross_5_20=VALUES(cross_5_20), cross_20_60=VALUES(cross_20_60), cross_60_120=VALUES(cross_60_120)
            """
            cursor = self.db_access.connection.cursor()
            # NaN 값을 None으로 변환하여 DB에 NULL로 저장되도록 함
            ma_data_cleaned = [tuple(None if pd.isna(v) else v for v in row) for row in ma_data]
            cursor.executemany(ma_query, ma_data_cleaned)
            self.db_access.connection.commit()
            logging.info(f"Saved/Updated {cursor.rowcount} Moving Average records for company_id {company_id}.")
            cursor.close()

        except Exception as e:
            logging.error(f"Error calculating/saving indicators for company_id {company_id}: {e}")