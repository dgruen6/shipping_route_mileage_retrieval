import logging
from sys import platform
from typing import Optional, List
import pyodbc

import pandas as pd


class DatabaseConnector:
    def __init__(self, server: str, database: str, username: str, password: str):
        self.conn: Optional[pyodbc] = None
        self.cursor: Optional[pyodbc] = None
        self._server: str = server
        self._database: str = database
        self._username: str = username
        self._password: str = password

    def __repr__(self):
        return f">{self._server}< database"

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
                   f"SERVER={self._server};" \
                   f"DATABASE={self._database};" \
                   f"UID={self._username};" \
                   f"PWD={self._password}"
        try:
            self.conn = pyodbc.connect(conn_str)
        except:
            logging.info(f"Could not connect to >{self._server}< SQL server")
            raise ConnectionError
        else:
            self.cursor = self.conn.cursor()
            return True

    def fetch(self, sql_statement: str) -> Optional[pd.DataFrame]:
        """
        Execute a SQL statement with given connection. You need to call the connect()-method before!

        :param sql_statement: SQL statement string
        :return: True if query was successful, False otherwise
        """
        result_df = pd.read_sql(sql_statement, self.conn)
        logging.info(f"Successfully fetched data from >{self._server}< SQL server")
        return result_df

    def commit(self, sql_statement: str, insert_data: List):
        """
        Insert routes into database

        :param sql_statement: Insert SQL query
        :param insert_data: Data to be inserted into database
        :return:
        """
        for row in insert_data:
            try:
                self.cursor.execute(sql_statement, *row)
                self.conn.commit()
            except:
                print("Error during database insertion")

    def close(self) -> None:
        """
        Close connection to database

        :return: None
        """
        self.conn.close()
