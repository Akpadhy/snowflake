import unittest
from pyspark.sql import SparkSession
from dfwriter.snowflake_conn import Snowflake

#  need to find way of testing
class SnowflakeTesting(unittest.TestCase):

    def test_snowflake_inte(self):
        sfOptions = {
            "sfURL": "tsdb-caifhmdlgrdsp.global.snowflakecomputing.com",
            "sfUser": "batchjob",
            "sfDatabase": "daily",
            "sfSchema": "Fact_Table",
            "sfWarehouse": "SPARK_WAREHOUSE",
            "sfRole": "DE"
            }
        spark = SparkSession.builder.master("local[4]").appName("snowflake_test").config("spark.driver.bindAddress","localhost").getOrCreate()
        df = spark.createDataFrame(
            [
                ("1", 1.0, 111111, 222222),
                ("2", 2.0, 333333, 44444),
            ],
            ['key', 'value', 'win_start', 'win_end']
        )
        sf = Snowflake(sf_options,"/Users/test/Downloads/rsa_key_test_private.pem")
        sf.write_data(df,"table_test")

if __name__ == '__main__':
    unittest.main()
