import logging
from sys import platform
from typing import Optional
import pyodbc

import pandas as pd


class DatabaseConnector:
    def __init__(self, server: str, database: str, username: str, password: str):
        self.conn: Optional[pyodbc] = None
        self.cursor: Optional[pyodbc] = None
        self.server: str = server
        self.database: str = database
        self.username: str = username
        self.password: str = password

    def __repr__(self):
        return f">{self.server}< database"

    def connect(self) -> bool:
        """
        Sets the cursor to the destination database.
        After calling this function, a command can be sent with self.cursor.execute()

        :return: True if connection to database is established, False otherwise
        """
        if platform == "darwin":  # MacOS
            driver = 'ODBC Driver 17 for SQL Server'
        else:  # Windows
            driver = 'SQL Server'
        conn_str = f"DRIVER={{{driver}}};" \
                   f"SERVER={self.server};" \
                   f"DATABASE={self.database};" \
                   f"UID={self.username};" \
                   f"PWD={self.password}"
        try:
            self.conn = pyodbc.connect(conn_str)
        except:
            logging.info(f"Could not connect to >{self.server}< SQL server")
            raise ConnectionError
        else:
            self.cursor = self.conn.cursor()
            logging.info(f"Successfully connected to >{self.server}< SQL server")
            return True

    def fetch(self, sql_statement: str) -> Optional[pd.DataFrame]:
        """
        Execute a SQL statement with given connection. You need to call the connect()-method before!

        :param sql_statement: SQL statement string
        :return: True if query was successful, False otherwise
        """
        result_df = pd.read_sql(sql_statement, self.conn)
        return result_df

    def commit(self, sql_statement: str):
        """
        Insert into database

        :param sql_statement:
        :return:
        """
        #TODO: implement this!
        pass

    def close(self) -> None:
        """
        Close connection to database

        :return: None
        """
        self.conn.close()
