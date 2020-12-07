import logging
from pyspark.sql import functions as F
import time

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
logger = logging.getLogger(__name__)

#Your connection string based on the setup
sf_options_default = {
    "sfURL": "tsdb-caifhmdlgrdsp.global.snowflakecomputing.com",
    "sfUser": "batch-job,
    "sfDatabase": "daily",
    "sfSchema": "Fact_Table",
    "sfWarehouse": "SPARK_WAREHOUSE",
    "sfRole": "DE"
}

class Snowflake:
    def __init__(self, private_key_file,sf_options=sf_options_default):
        self.sf_options = sf_options
        self.key_parser(private_key_file)

    def key_parser(self, file_path):
        with open(file_path, "r") as key:
            private_key = key.read()
            private_key = private_key.replace("-----BEGIN RSA PRIVATE KEY-----", "")
            private_key = private_key.replace("-----END RSA PRIVATE KEY-----", "")
            private_key = private_key.replace("[\n\r]", "")
            private_key = private_key.strip()
            self.sf_options.update({'pem_private_key': private_key})

    #Write Data into table        
    def write_data(self, dataframe, table):
        dataframe.write.format(SNOWFLAKE_SOURCE_NAME).options(**self.sf_options).option("dbtable", table).mode('overwrite').save()

    #Read Data from table
    def read_data(self, query,spark):
        df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**self.sf_options).option("query", query).load()
        return df

    #DDL, DML opeerations on table
    def dml_ops(self,query):
        spark.sparkContext._jvm.net.snowflake.spark.snowflake.Utils.runQuery(self.sf_options, query)