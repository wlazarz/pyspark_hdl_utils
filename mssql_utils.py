import pymssql
from typing import Any, Union, List, Dict, Optional, Tuple
from utils.logger import Logger


class MSSQLConnector:
    def __init__(self, logger: Logger, server: str, database: str, username: str = None, password: str = None):

        self.logger = logger
        self.server = server
        self.database = database

        self.username = username
        self.password = password

        self.conn: pymssql.Connection = self.connect()

    def connect(self):
        try:
            connection = pymssql.connect(server=self.server, user=self.username, password=self.password,
                                         database=self.database)
            if connection is None:
                raise ValueError("Unsuccessful connect")
            else:
                return connection
        except Exception as e:
            self.logger.info(f"Failed to connect: {e}")

    def check_if_table_exists(self, table_name: str) -> bool:
        """
        Returns True if table exists, False otherwise

        Arguments:
            table_name: name of table

        Returns:
            True if table exists, False otherwise
        """
        cursor = self.conn.cursor()
        query = f'''SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}';'''
        cursor.execute(query)
        table_exists = cursor.fetchone() is not None
        cursor.close()
        return table_exists

    def execute_query(self, query):
        """
        Executes query without returning result (mainly for DDL, DML operations).

        Arguments:
            query: operation query
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        cursor.close()

    def select_query(self, query: str, params: Optional[Tuple] = None, one: bool = True):
        """
        Selects records from database.

        Arguments:
            query: sql query
            params: query params
            one: if True than one record is returned, else many

        Return:
            list of table records or single record
        """
        cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        if one:
            result = cursor.fetchone()
        else:
            result = cursor.fetchall()
        cursor.close()

        return result

    def create_table_if_not_exists(self, table: str, query: str):
        """
        Creates table if not exists

        Arguments:
            table: name of table
            query: query creating table
        """
        if not self.check_if_table_exists(table):
            self.logger.info(f"Creating table {table}...")
            self.execute_query(query)

    def insert_into_table_from_dictionary(self, table_name: str, data: Union[List[Dict[str, Any]], Dict[str, Any]]):
        """
        Inserts a record or list of records into a table

        Arguments:
            table_name: name of table
            data: list of records (dictionaries) or a single record to insert
        """
        if data:
            if isinstance(data, list):
                columns = '[' + '], ['.join(data[0].keys()) + ']'
                placeholders = ', '.join(['%s'] * len(data[0]))
                values = [tuple(None if v is None else v for v in d.values()) for d in data]
            else:
                columns = '[' + '], ['.join(data.keys()) + ']'
                placeholders = ', '.join(['%s'] * len(data))
                values = [tuple(None if v is None else v for v in data.values())]

            cursor = self.conn.cursor()
            query = f'''INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'''
            print(query)
            cursor.executemany(query, values)
            self.conn.commit()
            cursor.close()
        else:
            self.logger.warning(f"There is nothing to insert to table {table_name}")

