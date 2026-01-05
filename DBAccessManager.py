# dbaccess class for accessing MySQL database.
import mysql.connector
from mysql.connector import Error
import logging

class DBAccessManager:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect_to_mysql(self):
            """Connect to MySQL database (재연결 방지)"""
            if self.connection and self.connection.is_connected():
                return self.connection
            try:
                self.connection = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                if self.connection.is_connected():
                    db_info = self.connection.get_server_info()
                    logging.info(f"Connected to MySQL Server version {db_info}")
                    return self.connection
            except Error as e:
                logging.error(f"Error while connecting to MySQL: {e}")
                return None        

    def execute_query(self, query, params=None):
        """쿼리 실행"""
        if not self.connection or not self.connection.is_connected():
            logging.error("데이터베이스에 연결되어 있지 않습니다.")
            return None
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            self.connection.commit()
        except Error as e:
            logging.error(f"Query execution failed: {e}")
            return None
        finally:
            cursor.close()

    def execute_many_query(self, query, params_list):
        """ executemany를 사용하여 여러 데이터를 한번에 추가 """
        if not self.connection or not self.connection.is_connected():
            logging.error("데이터베이스에 연결되어 있지 않습니다.")
            return None
        
        cursor = self.connection.cursor()
        try:
            cursor.executemany(query, params_list)
            self.connection.commit()
            logging.info(f"{cursor.rowcount} records were inserted.")
            return cursor
        except Error as e:
            logging.error(f"Execute many query failed: {e}")
            return None
        finally:
            cursor.close()

    def fetch_one(self, query, params=None):
        """쿼리 실행 후 하나의 결과를 반환"""
        if not self.connection or not self.connection.is_connected():
            logging.error("데이터베이스에 연결되어 있지 않습니다.")
            return None
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result
        except Error as e:
            logging.error(f"Query execution failed: {e}")
            return None
        finally:
            cursor.close()

    def fetch_all(self, query, params=None):
        """쿼리 실행 후 모든 결과를 반환"""
        if not self.connection or not self.connection.is_connected():
            logging.error("데이터베이스에 연결되어 있지 않습니다.")
            return None
        
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            result = cursor.fetchall()
            return result
        except Error as e:
            logging.error(f"Query execution failed: {e}")
            return None
        finally:
            cursor.close()

    def close_connection(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()