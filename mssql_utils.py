import pymssql
from typing import Any, Union, List, Dict, Optional, Tuple
from logger import Logger


class MSSQLConnector:
    def __init__(self, logger: Logger, server: str, database: str,
                 username: Optional[str] = None, password: Optional[str] = None):
        """
        Initializes MSSQLConnector and establishes a connection.

        :param logger: Logger instance for logging.
        :param server: SQL Server hostname or IP.
        :param database: Name of the database to connect to.
        :param username: Optional username for authentication.
        :param password: Optional password for authentication.
        """
        self.logger = logger
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.conn: Optional[pymssql.Connection] = self.connect()

    def connect(self) -> Optional[pymssql.Connection]:
        """
        Establishes and returns a pymssql connection.

        :return: pymssql.Connection object or None if connection fails.
        """
        try:
            connection = pymssql.connect(
                server=self.server,
                user=self.username,
                password=self.password,
                database=self.database
            )
            return connection
        except Exception as e:
            self.logger.error(f"Failed to connect to MSSQL: {e}")
            return None

    def check_if_table_exists(self, table_name: str) -> bool:
        """
        Checks if a table exists in the database.

        :param table_name: Name of the table to check.
        :return: True if the table exists, False otherwise.
        """
        cursor = self.conn.cursor()
        query = f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = %s;"
        cursor.execute(query, (table_name,))
        exists = cursor.fetchone() is not None
        cursor.close()
        return exists

    def execute_query(self, query: str) -> None:
        """
        Executes a SQL query without returning results (e.g., DDL/DML).

        :param query: SQL query to execute.
        """
        cursor = self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
        cursor.close()

    def select_query(self, query: str, params: Optional[Tuple[Any, ...]] = None, one: bool = True) -> Union[Tuple, List[Tuple], None]:
        """
        Executes a SELECT query and fetches results.

        :param query: SQL query string.
        :param params: Optional parameters for the query.
        :param one: If True, returns one record; otherwise, returns all.
        :return: Fetched record(s) or None.
        """
        cursor = self.conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        result = cursor.fetchone() if one else cursor.fetchall()
        cursor.close()
        return result

    def create_table_if_not_exists(self, table: str, query: str) -> None:
        """
        Creates a table if it does not already exist.

        :param table: Name of the table.
        :param query: SQL query to create the table.
        """
        if not self.check_if_table_exists(table):
            self.logger.info(f"Creating table {table}...")
            self.execute_query(query)

    def insert_into_table_from_dictionary(self, table_name: str, data: Union[List[Dict[str, Any]], Dict[str, Any]]) -> None:
        """
        Inserts a record or list of records into a table.

        :param table_name: Name of the table.
        :param data: Single dictionary or list of dictionaries representing the data.
        """
        if not data:
            self.logger.warning(f"There is nothing to insert into table {table_name}")
            return

        cursor = self.conn.cursor()

        if isinstance(data, list):
            columns = '[' + '], ['.join(data[0].keys()) + ']'
            placeholders = ', '.join(['%s'] * len(data[0]))
            values = [tuple(None if v is None else v for v in d.values()) for d in data]
        else:
            columns = '[' + '], ['.join(data.keys()) + ']'
            placeholders = ', '.join(['%s'] * len(data))
            values = [tuple(None if v is None else v for v in data.values())]

        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        self.logger.info(f"Executing insert: {query}")
        cursor.executemany(query, values)
        self.conn.commit()
        cursor.close()