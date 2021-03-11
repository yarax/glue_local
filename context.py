from pyspark.sql import SparkSession
import os
import os.path
import re
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from .dynamicframe import DynamicFrame
import logging
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

class MockedClass:
    def __init__(self, root):
        self.root = root

    def from_catalog(self, database, table_name, redshift_tmp_dir = "", transformation_ctx = "", push_down_predicate = "", additional_options = {}, catalog_id = None, **kwargs):
        path = os.path.join(self.root, "source_data", database, table_name)
        return self.get_mocked_df(path)

    def get_mocked_df(self, path):
        df = None
        if os.path.isfile(path + ".json"):
            df = spark.read.json(path + ".json")
        if os.path.isfile(path + ".parquet"):
            df = spark.read.parquet(path + ".parquet")
        if df is None:
            raise Exception(f'None of json or parquet files with a given name were found in the {path} folder') 
        return DynamicFrame(df)


class Sink:
    def __init__(self, connection_type, path, enableUpdateCatalog, partitionKeys, root):
        self.root = root
        self.checkFilePath(path)
        
    def checkFilePath(self, path):
        path = path.replace("s3:/", self.root)
        self.path = path
        fng = re.search(r'\/([^\/]+)$', path)
        self.filename = fng.group(1)
        os.makedirs(path.replace(self.filename, ""), exist_ok=True)
    def setCatalogInfo(self, catalogDatabase, catalogTableName):
        print("skipping setCatalogInfo..")
    def setFormat(self, format):
        self.format = format
    def writeFrame(self, df):
        # this must be json because glueparquet format is not supported locally
        df.write.format("json").save(self.path)

class Logger:
    def info(self, str):
        print(str)
    def warn(self, str):
        print(str)
    def error(self, str):
        print(str)
    

class GlueContext(SparkContext):
    def __init__(self, SparkContext):
        self.sc = spark.sparkContext
        self.sqlc = SQLContext(self.sc)
        if ('GLUE_SOURCE_PATH' in os.environ):
            self.root = os.environ['GLUE_SOURCE_PATH']
        else:
            self.root = os.path.join(os.getcwd(), "tests")
            logging.warning("Environment variable GLUE_SOURCE_PATH was not found. Using default location: %s", self.root)
        self.create_dynamic_frame = MockedClass(self.root)
        self.write_dynamic_frame = MockedClass(self.root)
        self._glue_logger = Logger()

    def getSink(self, connection_type, path, enableUpdateCatalog, partitionKeys):
        return Sink(connection_type, path, enableUpdateCatalog, partitionKeys, self.root)

    def sql(self, query):
        return self.sqlc.sql(query)

    def create_dynamic_frame_from_options(self, connection_type, connection_options={},
                                          format=None, format_options={}, transformation_ctx = "", push_down_predicate= "",  **kwargs):
        if ('local_path' in connection_options):
            return self.create_dynamic_frame.get_mocked_df(connection_options['local_path'])
        else:
            raise Exception('In order to test the method locally please provide connection_options[local_path] as a path to your local data') 


    def getSource(self, connection_type, format = None, transformation_ctx = "", push_down_predicate= "", **options):
        print("the method getSource is not implemented")

    def get_catalog_schema_as_spark_schema(self, database = None, table_name = None, catalog_id = None):
        print("the method get_catalog_schema_as_spark_schema is not implemented")

    def create_dynamic_frame_from_catalog(self, database = None, table_name = None, redshift_tmp_dir = "",
                                          transformation_ctx = "", push_down_predicate="", additional_options = {},
                                          catalog_id = None, **kwargs):
        return self.create_dynamic_frame.from_catalog(database, table_name)

    def write_dynamic_frame_from_options(self, frame, connection_type, connection_options={},
                                         format=None, format_options={}, transformation_ctx = ""):
        print("write_dynamic_frame_from_options was skipped locally..")
    

    def write_from_options(self, frame_or_dfc, connection_type,
                           connection_options={}, format={}, format_options={},
                           transformation_ctx = "", **kwargs):
        print("write_from_options was skipped locally..")

    def write_dynamic_frame_from_catalog(self, frame, database = None, table_name = None, redshift_tmp_dir = "",
                                         transformation_ctx = "", additional_options = {}, catalog_id = None, **kwargs):
        print("write_dynamic_frame_from_catalog was skipped locally..")

    def write_dynamic_frame_from_jdbc_conf(self, frame, catalog_connection, connection_options={},
                                           redshift_tmp_dir = "", transformation_ctx = "", catalog_id = None):
        print("write_dynamic_frame_from_jdbc_conf was skipped locally..")

    def write_from_jdbc_conf(self, frame_or_dfc, catalog_connection, connection_options={},
                             redshift_tmp_dir = "", transformation_ctx = "", catalog_id = None):
        print("write_from_jdbc_conf was skipped locally..")


    def convert_resolve_option(self, path, action, target):
        print("the method convert_resolve_option is not implemented")

    def extract_jdbc_conf(self, connection_name, catalog_id=None):
        print("the method extract_jdbc_conf is not implemented")

    def purge_table(self, database, table_name, options={}, transformation_ctx="", catalog_id=None):
        print("the method purge_table is not implemented")

    def purge_s3_path(self, s3_path, options={}, transformation_ctx=""):
        print("the method purge_s3_path is not implemented")

    def transition_table(self, database, table_name, transition_to, options={}, transformation_ctx="", catalog_id=None):
        print("the method transition_table is not implemented")

    def transition_s3_path(self, s3_path, transition_to, options={}, transformation_ctx=""):
        print("the method transition_s3_path is not implemented")
        
    def get_logger(self):
        return self._glue_logger