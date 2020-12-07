
**Introduction**

This is to use spark and snowflake integration.

**Using the Library**

Create Snowflake connection Object

```buildoutcfg
sf = Snowflake(private_key_file_path,sfOptions)

arguments details are as per below:

1. sfOptions is a parameter map for snowflake configuration: 
ex:
sfOptions = {
    "sfURL": "tsdb-caifhmdlgrdsp.global.snowflakecomputing.com",
    "sfUser": "batch-job",
    "sfDatabase": "daily",
    "sfSchema": "Fact_Table",
    "sfWarehouse": "SPARK_WAREHOUSE",
    "sfRole": "DE"
}

2.private_key_file_path - path of private key file for snowflake connection
for more info please go threw https://docs.snowflake.com/en/user-guide/spark-connector-use.html
```

Write dataframe to table:

```buildoutcfg
sf.write_data(dataframe,table_name)

arguments details are as per below:

1. dataframe - dataframe that needs to write to snowflake table
2. table_name - fully qualified name of snowflake table ex: STREAMS.UAT.DST_TESTING

```

Read dataframe from table:
```buildoutcfg
df=sf.read_data (query,spark) 

arguments details are as per below:

1. query - query that need to run on snowflake for fetch
2. spark - created spark session

```

**Sample Snowflake Integration :**

```python
from dfwriter.snowflake_conn import Snowflake

sf = Snowflake("/usr/share/snowflake/rsa_key.pem")

dataframe = spark.createDataFrame(
            [
                ("1", 1.0, 111111, 222222),
                ("2", 2.0, 333333, 44444),
            ],
            ['key', 'value', 'win_start', 'win_end']
        )

query = "select * from STREAMS.UAT.table_dummy where dt=2020-01-24"
sf.write_data(dataframe,"STREAMS.UAT.table_dummy")
output_dataframe = sf.read_data(query,spark)
output_dataframe.show()

```


**FAQs**

1. Is there any specific format of private key file for snowflake connection 
    * Private key format should be un-encrypted version of private key as snowflake support. 
    * For more info please follow link https://docs.snowflake.com/en/user-guide/spark-connector-use.html#using-key-pair-authentication.
   
2. Is there any default value for snowflake configurations:
    * As mentioned earlier library supports some default snowflake configuration as mentioned above.
   
3. Is there any default column getting append with each of snowflake table while write operation.
    * For audit purpose we are appending every snowflake table write with system_time_stamp column having system generated time stamp.

4. Is there any particular mode of write operation is supported in write_data library.
    * For security concern,we have only provided support for append mode write operation.

5. Should Snowflake table exist before write operation
    * There is no need to create snowflake table externally.
    * library itself will create snowflake table based on dataframe mapping that is going to get write.
    
6. Issue with Table or view not found or have not sufficient privilege
    * Check about username and role name in configuration ,it might happen due to insufficient privilege/access on username/Role with particular schema.
  