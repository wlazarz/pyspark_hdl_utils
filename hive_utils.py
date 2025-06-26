from time import time
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from typing import Union, Optional

from timer import Timer
from logger import Logger


class HiveUtils:
    def __init__(self, spark: SparkSession, logger: Logger):
        """
        :param spark: Spark session
        :param logger: Logger instance
        """
        self.spark = spark
        self.logger = logger
        self.timer = Timer()

    def insert_append_to_table(self, sdf: DataFrame, database: str, table_name: str,
                               format_: str = 'parquet') -> None:
        """
        Inserts data to Hive table. If the table doesn't exist, it is created.

        :param sdf: Spark DataFrame to insert
        :param database: Database name
        :param table_name: Table name
        :param format_: Format of table source files (default: parquet)
        """
        table_location = f"{database}.{table_name}"
        try:
            sdf.write.format(format_).insertInto(table_location)
        except AnalysisException:
            self.logger.error(f"Table {table_location} not found. Creating table...")
            self.create_table(sdf, table_location, format_)

    def insert_append_partition(self, sdf: DataFrame, database: str, table_name: str,
                                partitioning_column: str, format_: str = 'parquet') -> None:
        """
        Inserts data into a Hive table with partitioning. If the table doesn't exist, it is created.

        :param sdf: Spark DataFrame to insert
        :param database: Database name
        :param table_name: Table name
        :param partitioning_column: Column used for partitioning
        :param format_: Format of table source files (default: parquet)
        """
        table_location = f"{database}.{table_name}"
        try:
            sdf.write.mode("append").partitionBy(partitioning_column).format(format_).saveAsTable(table_location)
        except AnalysisException:
            self.logger.error(f"Table {table_location} not found. Creating table...")
            sdf.write.mode("overwrite").partitionBy(partitioning_column).format(format_).saveAsTable(table_location)

    def create_table(self, sdf: DataFrame, table_location: str, format_: str = 'parquet') -> None:
        """
        Creates a Hive table using the schema from the given DataFrame.

        :param sdf: Spark DataFrame
        :param table_location: Full table identifier (database.table_name)
        :param format_: Format of table source files (default: parquet)
        """
        try:
            start = time()
            sdf.write.format(format_).saveAsTable(table_location)
            self.logger.info(f"Hive table created successfully, time: {self.timer.time(start)}")
        except AnalysisException as e:
            self.logger.error(f"Failed to create Hive table {table_location}: {e}")

    def create_overwrite_table(self, sdf: DataFrame, database: str, table_name: str,
                               format_: str = 'parquet') -> None:
        """
        Creates or overwrites a Hive table.

        :param sdf: Spark DataFrame
        :param database: Database name
        :param table_name: Table name
        :param format_: Format of table source files (default: parquet)
        """
        table_location = f"{database}.{table_name}"
        self.logger.info(f"Saving DataFrame as Hive table {table_location}")
        start = time()
        try:
            sdf.write.format(format_).mode("overwrite").saveAsTable(table_location)
            self.logger.info(f"Overwrite success, time: {self.timer.time(start)}")
        except AnalysisException:
            self.logger.error(f"Table {table_location} not found. Creating table...")
            self.create_table(sdf, table_location, format_)

    def check_if_table_exists(self, database: str, table_name: str) -> bool:
        """
        Checks if a Hive table exists.

        :param database: Database name
        :param table_name: Table name
        :return: True if table exists, else False
        """
        return self.spark._jsparkSession.catalog().tableExists(database, table_name)

    def save_df_as_table_if_not_exists(self, sdf: DataFrame, database: str, table_name: str,
                                       format_: str = 'parquet') -> None:
        """
        Saves DataFrame as Hive table only if it does not already exist.

        :param sdf: Spark DataFrame to insert
        :param database: Database name
        :param table_name: Table name
        :param format_: Format of table source files (default: parquet)
        """
        table_location = f"{database}.{table_name}"
        self.logger.info(f"Saving DataFrame as Hive table {table_location}")
        if not self.check_if_table_exists(database, table_name):
            try:
                start = time()
                sdf.write.format(format_).saveAsTable(table_location)
                self.logger.info(f"Table creation success, time: {self.timer.time(start)}")
            except AnalysisException as e:
                self.logger.error(f"Table creation failed: {e}")

    def create_table_with_schema(self, schema: str, database: str, table_name: str,
                                 partition_column: Optional[str] = None) -> None:
        """
        Creates a Hive table using raw schema SQL.

        :param schema: Schema definition as SQL string
        :param database: Database name
        :param table_name: Table name
        :param partition_column: Optional partition column
        """
        if partition_column:
            query = (
                f"CREATE TABLE IF NOT EXISTS {database}.{table_name} ({schema}) "
                f"PARTITIONED BY({partition_column}) "
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'"
            )
        else:
            query = (
                f"CREATE TABLE IF NOT EXISTS {database}.{table_name} ({schema}) "
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'"
            )

        self.logger.info(f"Creating Hive table {table_name}")
        start = time()
        try:
            self.spark.sql(query)
            self.logger.info(f"Hive table created, time: {self.timer.time(start)}")
        except AnalysisException as e:
            self.logger.error(f"Table creation query failed: {e}")

    def drop_partition(self, database: str, table_name: str,
                       partition_name: str, partition_value: Union[int, str]) -> None:
        """
        Drops a partition from a Hive table.

        :param database: Database name
        :param table_name: Table name
        :param partition_name: Partition column name
        :param partition_value: Partition value to drop
        """
        query = (
            f"ALTER TABLE {database}.{table_name} "
            f"DROP IF EXISTS PARTITION ({partition_name}='{partition_value}')"
        )
        try:
            self.spark.sql(query)
        except Exception as e:
            self.logger.error(
                f"Failed to drop partition {partition_value} from {database}.{table_name}. Error: {e}"
            )
