import subprocess
import json
from typing import List, Union, Dict, Any
#import xml.etree.ElementTree as ET
from pyspark.sql.types import StructType, Row
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext

#from utils.xsd_tree import serialize_tree, TreeNode
from utils.logger import Logger
from utils.timer import Timer


class HDFSUtils:

    def __init__(self, spark: SparkSession, sc: SparkContext, logger: Logger):
        self.spark = spark
        self.sc = sc
        self.logger = logger
        self.timer = Timer()
        self.hadoop_conf = self.sc._jsc.hadoopConfiguration()
        self.fs = self.sc._jvm.org.apache.hadoop.fs.FileSystem.get(self.hadoop_conf)

    def hdfs_list(self, path: str) -> List[str]:
        """
        Lists all files under given path in HDFS.
        :param path: path that contains files
        :return: list of filenames
        """
        try:
            list_status = self.fs.listStatus(self.spark._jvm.org.apache.hadoop.fs.Path(path))
            return [file.getPath().toString() for file in list_status]
        except Exception as e:
            self.logger.error(f"Files listing from {path} failed with error:\n{e}")

    def hdfs_rename(self, old_path: str, new_path: str) -> None:
        """
        Renames a file saved in HDFS.
        :param old_path: old file path with name
        :param new_path: new file path with name, path do directory should be the same as old path
        """

        try:
            self.fs.rename(
                self.spark._jvm.org.apache.hadoop.fs.Path(old_path),
                self.spark._jvm.org.apache.hadoop.fs.Path(new_path)
            )
        except Exception as e:
            self.logger.error(f"Rename {old_path} to {new_path} failed with error:\n{e}")

    def hdfs_move_file(self, old_path: str, new_path: str) -> None:
        """
        Moves file from one location to another.
        :param old_path: old file path with name
        :param new_path: new file path with name, path do directory should be the same as old path
        """

        if not self.check_if_path_exists(new_path):
            self.fs.mkdirs(self.spark._jvm.org.apache.hadoop.fs.Path(new_path))

        try:
            self.fs.rename(
                self.spark._jvm.org.apache.hadoop.fs.Path(old_path),
                self.spark._jvm.org.apache.hadoop.fs.Path(new_path)
            )
        except Exception as e:
            self.logger.error(f"Rename {old_path} to {new_path} failed with error:\n{e}")

    def hdfs_move_directory_content(self, src_path: str, dst_path: str) -> None:
        """
        Moves directory content from one loction to another.
        :param src_path: old directory path
        :param dst_path: new directory path
        """

        src = self.sc._jvm.org.apache.hadoop.fs.Path(src_path)
        dst = self.sc._jvm.org.apache.hadoop.fs.Path(dst_path)

        if not self.check_if_path_exists(dst_path):
            self.fs.mkdirs(self.spark._jvm.org.apache.hadoop.fs.Path(dst_path))

        def move_contents(src, dst):
            file_status_list = self.fs.listStatus(src)

            for file_status in file_status_list:
                src_path = file_status.getPath()
                dst_path = self.sc._jvm.org.apache.hadoop.fs.Path(dst, src_path.getName())
                if file_status.isDirectory():
                    if not self.fs.exists(dst_path):
                        self.fs.mkdirs(dst_path)
                    move_contents(src_path, dst_path)
                else:
                    self.fs.rename(src_path, dst_path)

        move_contents(src, dst)

    def check_if_path_exists(self, directory_path: str) -> bool:
        """
        Checks if directory exists
        :param directory_path: directory path string
        return: True if directory exists else False
        """
        exists = self.fs.exists(self.spark._jvm.org.apache.hadoop.fs.Path(directory_path))
        return exists

    def hdfs_delete(self, path: str) -> None:
        """
        Removes file from HDFS.
        :param path: path to file
        """
        try:
            if self.check_if_path_exists(path):
                self.fs.delete(self.spark._jvm.org.apache.hadoop.fs.Path(path), True)
            else:
                self.logger.info(f"File does not exists in this location {path}")
        except Exception as e:
            self.logger.error(f"Deleting file {path} failed with error:\n{e}")

    def spark_read_xml(self, xml_path: str, schema: StructType, root_tag: str) -> DataFrame:
        """
        Reads XML file from HDFS.
        :param xml_path: path to xml file (on hdfs)
        :param schema: pyspark schema describing xml file
        :param root_tag: root tag of xml file. Tag must be compatible with schema.
        :return: spark rdd
        """
        if self.check_if_path_exists(xml_path):
            return (self.spark.read.format("xml")
                    .schema(schema)
                    .option("ignoreNamespace", True)
                    .option("rowTag", root_tag)
                    .option("mode", "FAILFAST")
                    .load(xml_path))

    def read_directory_files(self, directory: str) -> List[bytes]:
        """
        List files available in hdfs directory.
        :param directory: path to directory
        :return: list of files
        """
        cmd = f'hdfs dfs -ls {directory}'
        try:
            return subprocess.check_output(cmd, shell=True).strip().split('\n')
        except Exception as e:
            self.logger.error(f'Reading files from {directory} failed with error {e}')

    '''def save_tree_to_json(self, tree: TreeNode, path_: str) -> None:
        """
        Saves tree python structure to hdfs as json file.
        Tree need to has implemented method serialize_tree(), which transforms tree to dictionary.
        :param tree: tree class instance
        :param path_: path to save tree
        """

        serialized_tree = serialize_tree(tree)
        json_string = json.dumps(serialized_tree)
        rdd = self.sc.parallelize([json_string])
        try:
            rdd.saveAsTextFile(path_)
        except:
            self.logger.info("Tree already exists, deleting tree...")
            try:
                self.hdfs_delete(path_)
                rdd.saveAsTextFile(path_)
                self.logger.info(f"Tree has been saved as {path_}")
            except Exception as e1:
                self.logger.error(f"Saving failed, error: {e1}")

    def read_xsd(self, path_: str) -> ET.Element:
        """
        Reads xsd file from location specified. It can be hdfs or local directory.
        :param path_: path to xsd file
        :return: ET tree object
        """
        try:
            if self.check_if_path_exists(path_):
                xsd_rdd = self.sc.textFile(path_).collect()
                return ET.fromstring('\n'.join(xsd_rdd))
            else:
                self.logger.info(f"File does not exists in this location {path_}")

        except Exception as e:
            self.logger.error(str(e))
            try:
                self.logger.info("Try to read from local")
                return ET.parse(path_).getroot()
            except Exception as e1:
                self.logger.error(f"""File not exists or You don't have permission to read from this location 
                      or error while parsing to etree instance: {e1}""")'''

    def save_string_to_json_hdfs(self, json_string: str, path_: str) -> None:
        """
        Saves the json file to the specified location. It has to be hdfs directory
        :param json_string: json string, for example result of method .json() on pyspark schema
        :param path_: path to write json file
        """

        try:
            hdfs_path = self.sc._gateway.jvm.org.apache.hadoop.fs.Path(path_)
            json_bytes = bytearray(json_string, "utf-8")
            output_stream = self.fs.create(hdfs_path, True)
            output_stream.write(json_bytes)
            output_stream.close()

        except Exception as e:
            self.logger.error(f"""Location does not exist or You don't have permission to save file in specified 
            location: {e}""")

    def read_pyspark_json_schema(self, path_: str) -> StructType:
        """
        Reads the json file from the specified location and transforms it into the Pyspark schema.
        It has to be hdfs location.
        :param path_: path to json file
        :return: pyspark schema
        """
        try:
            json_rdd = self.sc.textFile(path_)
            json_string = json_rdd.collect()[0]
            schema_json_read = json.loads(json_string)
            schema_read = StructType.fromJson(schema_json_read)
            return schema_read

        except Exception as e:
            self.logger.error(f"""File not exists or You don't have permission to read from this location or error 
            while parsing to StructType: {e}""")

    def get_file_size(self, file_path: str) -> Union[bytes, int]:
        """
        Returns size of file in bytes.
        :param file_path: path to file
        :return: size of file
        """
        try:
            file_path = self.sc._jvm.org.apache.hadoop.fs.Path(file_path)
            file_status = self.fs.getFileStatus(file_path)
            file_size = file_status.getLen()
            return round(file_size / (1024 * 1024), 5)

        except Exception as e:
            self.logger.error(f"Error: {e}")

    def save_bytes_as_file(self, bytes_file: bytes, destination_path: str) -> None:
        """
        Saves bytes file to specified location with specified extension
        :param bytes_file: file to be saved
        :param destination_path: path to save file with file extension
        """
        destination_path = self.sc._gateway.jvm.org.apache.hadoop.fs.Path(destination_path)
        output_stream = self.fs.create(destination_path, True)
        output_stream.write(bytes_file)
        output_stream.close()

    def read_xml_as_string(self, path: str) -> str:
        """
        Reads xml file from hdfs location as string.
        :param path: path to xml file
        :return: xml string
        """
        if self.check_if_path_exists(path):
            try:
                rdd = self.sc.textFile(path)
                return "\n".join(rdd.collect())
            except Exception as e:
                self.logger.error(f"""You don't have permission to read from this location or error while 
                                         parsing to string: {e}""")
        else:
            self.logger.info(f"There is no such file {path}")

    '''def read_tree_from_hdfs(self, path_: str) -> TreeNode:
        """
        Reads json file that contains xsd tree structure and parse it to TreeNode
        :param path_: path to tree
        :return: TreeNode object
        """

        def struct_to_tree(root: Row, tree: Union[TreeNode, None]) -> TreeNode:
            root = root.asDict()
            node = TreeNode(root["tag"], root["attributes"].asDict())
            node.path = root["path"]
            node.parent = tree
            if root.get("children"):
                for child in root["children"]:
                    node.children.append(struct_to_tree(child, node))
            return node

        struct = self.spark.read.json(path_).collect()[0]
        return struct_to_tree(struct, None)'''

    def read_json_as_dict(self, path_: str) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Reads multiple nested json file from HDFS to python dictionary
        :param path_: path to tree
        :return: python dict
        """

        def row_to_dict(row):
            if isinstance(row, dict):
                return {key: row_to_dict(value) for key, value in row.items()}
            elif isinstance(row, list):
                return [row_to_dict(item) for item in row]
            elif hasattr(row, "asDict"):
                return {key: row_to_dict(value) for key, value in row.asDict().items()}
            else:
                return row

        df = self.spark.read.option("multiline", "true").json(path_)
        rows = df.collect()
        result_dict = [row_to_dict(row) for row in rows]

        if len(rows) < 2:
            return result_dict[0]

        return result_dict
