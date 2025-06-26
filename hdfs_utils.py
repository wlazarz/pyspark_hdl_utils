import subprocess
import json
from typing import List, Union, Dict, Any, Optional
from pyspark.sql.types import StructType, Row
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext

from logger import Logger
from timer import Timer

from pathlib import Path
from py4j.java_gateway import JavaObject

PathLike = Union[str, bytes, Path, JavaObject]


class HDFSUtils:
    def __init__(self, spark: SparkSession, sc: SparkContext, logger: Logger):
        self.spark = spark
        self.sc = sc
        self.logger = logger
        self.timer = Timer()
        self.hadoop_conf = self.sc._jsc.hadoopConfiguration()
        self.fs = self.sc._jvm.org.apache.hadoop.fs.FileSystem.get(self.hadoop_conf)

    def _to_java_path(self, path: PathLike) -> JavaObject:
        if isinstance(path, Path):
            path = str(path)
        if isinstance(path, (str, bytes)):
            return self.sc._jvm.org.apache.hadoop.fs.Path(str(path))
        return path

    def hdfs_list(self, path: PathLike) -> Optional[List[str]]:
        """
        Lists all files under a given path in HDFS.

        :param path: File or directory path (str, bytes, Path, or Java Path).
        :return: List of file paths as strings.
        """
        try:
            list_status = self.fs.listStatus(self._to_java_path(path))
            return [file.getPath().toString() for file in list_status]
        except Exception as e:
            self.logger.error(f"Files listing from {path} failed with error:\n{e}")

    def hdfs_rename(self, old_path: PathLike, new_path: PathLike) -> None:
        """
        Renames a file saved in HDFS.

        :param old_path: Old file path.
        :param new_path: New file path.
        """
        try:
            self.fs.rename(self._to_java_path(old_path), self._to_java_path(new_path))
        except Exception as e:
            self.logger.error(f"Rename {old_path} to {new_path} failed with error:\n{e}")

    def hdfs_move_file(self, old_path: PathLike, new_path: PathLike) -> None:
        """
        Moves a file from one location to another.

        :param old_path: Source file path.
        :param new_path: Destination file path.
        """
        if not self.check_if_path_exists(new_path):
            self.fs.mkdirs(self._to_java_path(new_path))
        try:
            self.fs.rename(self._to_java_path(old_path), self._to_java_path(new_path))
        except Exception as e:
            self.logger.error(f"Move {old_path} to {new_path} failed with error:\n{e}")

    def hdfs_move_directory_content(self, src_path: PathLike, dst_path: PathLike) -> None:
        """
        Moves the contents of one directory to another in HDFS.

        :param src_path: Source directory path.
        :param dst_path: Destination directory path.
        """
        src = self._to_java_path(src_path)
        dst = self._to_java_path(dst_path)

        if not self.check_if_path_exists(dst_path):
            self.fs.mkdirs(dst)

        def move_contents(source, destination):
            for file_status in self.fs.listStatus(source):
                current_src = file_status.getPath()
                current_dst = self.sc._jvm.org.apache.hadoop.fs.Path(destination, current_src.getName())
                if file_status.isDirectory():
                    if not self.fs.exists(current_dst):
                        self.fs.mkdirs(current_dst)
                    move_contents(current_src, current_dst)
                else:
                    self.fs.rename(current_src, current_dst)

        move_contents(src, dst)

    def check_if_path_exists(self, path: PathLike) -> bool:
        """
        Checks if a given path exists in HDFS.

        :param path: Path to check.
        :return: True if exists, else False.
        """
        return self.fs.exists(self._to_java_path(path))

    def hdfs_delete(self, path: PathLike) -> None:
        """
        Deletes a file or directory in HDFS.

        :param path: Path to delete.
        """
        try:
            if self.check_if_path_exists(path):
                self.fs.delete(self._to_java_path(path), True)
            else:
                self.logger.info(f"Path does not exist: {path}")
        except Exception as e:
            self.logger.error(f"Deleting {path} failed with error:\n{e}")

    def spark_read_xml(self, xml_path: PathLike, schema: StructType, root_tag: str) -> Optional[DataFrame]:
        """
        Reads an XML file into a Spark DataFrame.

        :param xml_path: HDFS path to XML file.
        :param schema: Spark schema.
        :param root_tag: Root tag in XML file.
        :return: Spark DataFrame.
        """
        if self.check_if_path_exists(xml_path):
            return (self.spark.read.format("xml")
                    .schema(schema)
                    .option("ignoreNamespace", True)
                    .option("rowTag", root_tag)
                    .option("mode", "FAILFAST")
                    .load(str(xml_path)))

    def read_directory_files(self, directory: PathLike) -> Optional[List[bytes]]:
        """
        Lists files in an HDFS directory using the shell.

        :param directory: Path to directory.
        :return: List of file info lines as bytes.
        """
        cmd = f'hdfs dfs -ls {str(directory)}'
        try:
            return subprocess.check_output(cmd, shell=True).strip().split(b'\n')
        except Exception as e:
            self.logger.error(f'Reading files from {directory} failed with error {e}')

    def save_string_to_json_hdfs(self, json_string: str, path_: PathLike) -> None:
        """
        Saves a JSON string to HDFS.

        :param json_string: JSON content as string.
        :param path_: Path to save the file.
        """
        try:
            path_obj = self._to_java_path(path_)
            json_bytes = bytearray(json_string, "utf-8")
            output_stream = self.fs.create(path_obj, True)
            output_stream.write(json_bytes)
            output_stream.close()
        except Exception as e:
            self.logger.error(f"Saving JSON failed: {e}")

    def read_pyspark_json_schema(self, path_: PathLike) -> Optional[StructType]:
        """
        Reads a PySpark schema from a JSON file.

        :param path_: HDFS path to schema JSON.
        :return: StructType schema.
        """
        try:
            json_rdd = self.sc.textFile(str(path_))
            json_string = json_rdd.collect()[0]
            return StructType.fromJson(json.loads(json_string))
        except Exception as e:
            self.logger.error(f"Error reading schema from {path_}: {e}")

    def get_file_size(self, file_path: PathLike) -> Optional[float]:
        """
        Gets the size of a file in MB.

        :param file_path: Path to the file.
        :return: File size in megabytes.
        """
        try:
            file_status = self.fs.getFileStatus(self._to_java_path(file_path))
            return round(file_status.getLen() / (1024 * 1024), 5)
        except Exception as e:
            self.logger.error(f"Error getting file size: {e}")

    def save_bytes_as_file(self, bytes_file: bytes, destination_path: PathLike) -> None:
        """
        Saves a byte object as a file in HDFS.

        :param bytes_file: File content.
        :param destination_path: File destination.
        """
        try:
            path_obj = self._to_java_path(destination_path)
            output_stream = self.fs.create(path_obj, True)
            output_stream.write(bytes_file)
            output_stream.close()
        except Exception as e:
            self.logger.error(f"Saving bytes to {destination_path} failed: {e}")

    def read_xml_as_string(self, path: PathLike) -> Optional[str]:
        """
        Reads an XML file from HDFS as a string.

        :param path: Path to XML file.
        :return: XML content as string.
        """
        if self.check_if_path_exists(path):
            try:
                rdd = self.sc.textFile(str(path))
                return "\n".join(rdd.collect())
            except Exception as e:
                self.logger.error(f"Reading XML as string failed: {e}")
        else:
            self.logger.info(f"File not found: {path}")

    def read_json_as_dict(self, path_: PathLike) -> Optional[Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Reads a JSON file into a Python dictionary or list.

        :param path_: HDFS path to JSON.
        :return: Parsed JSON as dict or list of dicts.
        """
        def row_to_dict(row):
            if isinstance(row, dict):
                return {k: row_to_dict(v) for k, v in row.items()}
            elif isinstance(row, list):
                return [row_to_dict(i) for i in row]
            elif hasattr(row, "asDict"):
                return {k: row_to_dict(v) for k, v in row.asDict().items()}
            else:
                return row

        try:
            df = self.spark.read.option("multiline", "true").json(str(path_))
            rows = df.collect()
            result = [row_to_dict(row) for row in rows]
            return result[0] if len(result) == 1 else result
        except Exception as e:
            self.logger.error(f"Reading JSON as dict failed: {e}")