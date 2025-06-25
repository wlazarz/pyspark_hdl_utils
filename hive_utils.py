from time import time
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame
from typing import Union

from utils.timer import Timer


class HiveUtils:

    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger
        self.timer = Timer()

    def insert_append_to_table(self, sdf: DataFrame, database: str, table_name: str, format_: str = 'parquet') -> None:
        """
        Inserts data to hive table. If table doesn't exist then is created first.
        :param sdf: spark dataframe to insert
        :param database: database name
        :param table_name: table name
        :param format_: format of table source files
        """

        table_location = f"{database}.{table_name}"
        try:
            sdf.write.format(format_).insertInto(table_location)
        except AnalysisException as e:
            self.logger.error(f"Table {database}.{table_name} not found. Creating table...")
            self.create_table(sdf, table_location, format_)

    def insert_append_partition(self, sdf: DataFrame, database: str, table_name: str, partitioning_column: str,
                                format_: str = 'parquet') -> None:
        """
        Inserts data to hive table. If table doesn't exist then is created first.
        :param sdf: spark dataframe to insert
        :param database: database name
        :param table_name: table name
        :param partitioning_column: partitioning column name
        :param format_: format of table source files
        """

        table_location = f"{database}.{table_name}"

        try:
            (sdf.write.mode("append").partitionBy(partitioning_column).format(format_).
             saveAsTable(f"{database}.{table_name}"))

        except AnalysisException as e:
            self.logger.error(f"Table {table_location} not found. Creating table...")
            (sdf.write.mode("overwrite").partitionBy(partitioning_column).format(format_).
             saveAsTable(f"{database}.{table_name}"))

    def create_table(self, sdf: DataFrame, table_location: str, format_: str = 'parquet') -> None:
        """
        Created hive table
        :param sdf: spark dataframe to insert
        :param table_location: database_name.table_name
        :param format_: format of table source files
        """
        try:
            start = time()
            sdf.write.format(format_).saveAsTable(table_location)
            self.logger.info(f"Hive session (saveAsTable) process success, time: {self.timer.time(start)}")
        except AnalysisException as e:
            self.logger.error(f"Hive session (saveAsTable) to {table_location} process Failed with error: {e}")

    def create_overwrite_table(self, sdf: DataFrame, database: str, table_name: str, format_: str = 'parquet') -> None:
        """
        Inserts data to hive table. If table doesn't exist then is created first.
        If table exists when it is overwritten.
        :param sdf: spark dataframe to insert
        :param database: database name
        :param table_name: table name
        :param format_: format of table source files
        """

        table_location = f"{database}.{table_name}"
        self.logger.info(f"Saving dataframe as hive table {table_location}")
        start = time()
        try:
            sdf.write.format(format_).mode("overwrite").saveAsTable(table_location)
            self.logger.info(f"Hive session (saveAsTable overwrite) process success, time: {self.timer.time(start)}")
        except AnalysisException as e:
            self.logger.error(f"Table {table_location} not found. Creating table...")
            self.create_table(sdf, table_location)

    def check_if_table_exists(self, database: str, table_name: str) -> bool:
        """
        Returns True if table exists in database, else returns False
        :param database: database name
        :param table_name: table name
        :return: True or False
        """
        if self.spark._jsparkSession.catalog().tableExists(database, table_name):
            return True
        return False

    def save_df_as_table_if_not_exists(self, sdf: DataFrame, database: str, table_name: str,
                                       format_: str = 'parquet') -> None:
        """
        Saves data to hive table only if there is no such table.
        :param sdf: spark dataframe to insert
        :param database: database name
        :param table_name: table name
        :param format_: format of table source files
        """

        table_location = f"{database}.{table_name}"
        self.logger.info(f"Saving dataframe as hive table {table_location}")
        if not self.check_if_table_exists(database, table_name):
            try:
                start = time()
                sdf.write.format(format_).saveAsTable(table_location)
                self.logger.info(f"Hive session (saveAsTable for pandas dataframe) process success, "
                                 f"time: {self.timer.time(start)}")
            except AnalysisException as e:
                self.logger.error(f"Hive session (saveAsTable for pandas dataframe) process failed with exception: {e}")

    def create_table_with_schema(self, schema: str, database: str, table_name: str,
                                 partition_column: str = None) -> None:
        """
        Creates hive table using table schema. You can specify if table need to be partitioned.
        :param schema: table schema
        :param database: database name
        :param table_name: table name
        :param partition_column: partitioning column name
        """

        if partition_column:
            query = f"""CREATE TABLE IF NOT EXISTS {database}.{table_name} ({schema}) 
            PARTITIONED BY({partition_column})
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'"""
        else:
            query = f"""CREATE TABLE IF NOT EXISTS {database}.{table_name} ({schema})
            ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'"""

        self.logger.info(f"Creating hive table {table_name}")
        start = time()
        try:
            self.spark.sql(query)
            self.logger.info(f"Hive session (create table query) process success, time: {self.timer.time(start)}")
        except AnalysisException as e:
            self.logger.error(f"Hive session (create table query) process failed with exception: {e}")

    def drop_partition(self, database: str, table_name: str, partition_name: str, partition_value: Union[int, str]) \
            -> None:
        """
        Drop partition from table.
        :param database: database name
        :param table_name: table name
        :param partition_name: name of partitioning column
        :param partition_value: partition to drop
        """
        query = f"ALTER TABLE {database}.{table_name} DROP IF EXISTS PARTITION ({partition_name}='{partition_value}')"
        try:
            self.spark.sql(query)
        except Exception as e:
            self.logger.error(f"Deleting partition {partition_value} from table {database}.{table_name} failed. "
                              f"Error: {e}")
