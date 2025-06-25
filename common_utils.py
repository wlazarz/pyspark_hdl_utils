from __future__ import annotations
import json
import pandas as pd
from pandas import DataFrame as PDataFrame
from typing import Union, Dict
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from utils.xsd_tree import TreeNode, serialize_tree


def load_json(path_: str) -> None:
    """
    Reads json file
    :param path_: path to file
    :return: loaded json
    """
    try:
        return json.load(open(path_, "rb"))
    except Exception as e:
        print(f'Reading file {path_} failed with error {e}')


def save_json_tree(tree: TreeNode, output_path: str) -> None:
    """
    Saves the tree to a location on HDFS as a json file with a nested structure.
    :param tree: tree structure
    :param output_path: location on hdfs where the tree will be saved
    """
    serialized_tree = serialize_tree(tree)
    json_string = json.dumps(serialized_tree)
    try:
        with open(output_path, 'w') as f:
            json.dump(json_string, f)
    except Exception as e:
        print(e)


def parse_pandas_columns(df: PDataFrame, parser: Dict[str, str]) -> PDataFrame:
    """
    Parses column types in a pandas dataframe according to the parser dictionary.
    :param df: pandas dataframe
    :param parser: dictionary for pandas dataframe {column: typename}. Possible type names: 'int', 'string', 'decimal',
                   'number', 'time', 'gYear', 'date', 'dateTime', 'boolean'
    :return: pandas dataframe with parsed data types
    """

    for column in df.columns:
        if not column.startswith('index'):
            type_ = parser[column]
            if type_ in ['string', 'time', 'gYear']:
                df[column] = df[column].astype(str)
            elif type_ in ['number', 'int']:
                df[column] = df[column].astype(int)
            elif type_ == 'decimal':
                df[column] = df[column].astype(float)
            elif type_ in ['date', 'dateTime']:
                df[column] = pd.to_datetime(df[column], errors='coerce')
            elif type_ == 'boolean':
                df[column] = df[column].astype(bool)
        else:
            df[column] = df[column].astype(str)
    return df


def pandas_dtype_to_pyspark_dtype(pandas_dtype):
    """
    Parse data types from pandas types to pyspark types.
    :param pandas_dtype: type of pandas column
    :return: corresponding pyspark type
    """

    if pd.api.types.is_integer_dtype(pandas_dtype):
        return LongType()
    elif pd.api.types.is_float_dtype(pandas_dtype):
        return DoubleType()
    elif pd.api.types.is_bool_dtype(pandas_dtype):
        return BooleanType()
    elif pd.api.types.is_datetime64_any_dtype(pandas_dtype):
        return TimestampType()
    else:
        return StringType()


def pandas_to_pyspark(spark: SparkSession, df: PDataFrame) -> DataFrame:
    """
    Parses a pandas dataframe to a pyspark dataframe preserving the proper data types.
    :param spark: spark dataframe
    :param df: pandas dataframe
    :return: pyspark dataframe
    """
    schema = StructType([
        StructField(column, pandas_dtype_to_pyspark_dtype(dtype), True)
        for column, dtype in df.dtypes.items()
    ])

    pyspark_df = spark.createDataFrame(df, schema=schema).replace("None", None)
    return pyspark_df


def read_xml_as_string_from_hdfs(xml_path: Union[bytes, str]) -> Union[bytes, str]:
    sc = SparkContext.getOrCreate()
    rdd = sc.textFile(xml_path)
    xml_content = "\n".join(rdd.collect()).replace('&', '&amp;')
    return xml_content


def read_xml_as_string_from_local(xml_path: Union[bytes, str]) -> Union[bytes, str]:
    with open(xml_path, 'r') as f:
        xml_content = f.read()
    return xml_content


def read_xml_to_spark(spark: SparkSession, pyspark_schema: StructType, root_tag: str, xml_path: str) -> DataFrame:
    """
    Reads xml file based on pyspark schema to pyspark DataFrame. Xml file path must refer to HDFS.
    :param spark: spark session
    :param pyspark_schema: pyspark schema
    :param root_tag: root tag od xml file
    :param xml_path: path to xml file
    :return: pyspark dataframe with xml data
    """
    return (spark.read
            .format("xml")
            .schema(pyspark_schema)
            .option("ignoreNamespace", True)
            .option("rowTag", root_tag)
            .option("mode", "FAILFAST")
            .load(xml_path)
            )


def spark_to_mssql_type(spark_type):
    if isinstance(spark_type, StringType):
        return "VARCHAR(MAX)"
    elif isinstance(spark_type, IntegerType):
        return "INT"
    elif isinstance(spark_type, LongType):
        return "BIGINT"
    elif isinstance(spark_type, FloatType) or isinstance(spark_type, DoubleType):
        return "FLOAT"
    elif isinstance(spark_type, DecimalType):
        return "DECIMAL(38,10)"
    elif isinstance(spark_type, BooleanType):
        return "BIT"
    elif isinstance(spark_type, DateType):
        return "DATE"
    elif isinstance(spark_type, TimestampType):
        return "DATETIME"
    else:
        return "VARCHAR(MAX)"


def spark_to_mssql_schema(sdf, table):
    schema = sdf.schema
    columns_sql = ",\n".join([f"[{field.name}] {spark_to_mssql_type(field.dataType)}" for field in schema.fields])
    create_table_sql = f"CREATE TABLE {table} (\n{columns_sql}\n);"
    return create_table_sql
