# dbaccess class for accessing MySQL database
# This script loads stock item codes from FinanceDataReader, and 
# then loads daily price data for each stock item.

import numpy as np
import os
import datetime
import pandas as pd
import FinanceDataReader as fdr 
import mysql.connector
from mysql.connector import Error
import logging
from sqlalchemy import create_engine
import urllib.parse

class dbaccess:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self._sqlalchemy_engine = None  # SQLAlchemy 엔진 캐싱

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
        finally:
            cursor.close()

    def close_connection(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("MySQL connection is closed")        