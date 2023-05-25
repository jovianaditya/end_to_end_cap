from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Create New Database In Hive
spark.sql("create database dwh")
spark.sql("show databases").show()
spark.sql("use dwh")

# Execute HiveQL statement to create table

create_dim1 = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS dim1 (
        SK_Vendor BIGINT,
        VendorID BIGINT,
        VendorName STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/jovianaditya/df_1'
'''

create_dim2 = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS dim2 (
        SK_Payment BIGINT,
        Payment_type DOUBLE,
        PaymentMethod STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/jovianaditya/df_2'
'''

create_dim3 = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS dim3 (
        SK_Rate BIGINT,
        RateCodeID DOUBLE,
        Rate_Desc STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/jovianaditya/df_3'
'''
create_dim4 = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS dim4 (
        SK_Trip BIGINT,
        Trip_type DOUBLE,
        Trip_Desc STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/jovianaditya/df_4'
'''

create_dim5 = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS dim5 (
        SK_Flag BIGINT,
        Store_and_fwd_flag STRING,
        Flag_Desc STRING)
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/jovianaditya/df_5'
'''

create_fact = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS fact (
        SK_Vendor BIGINT,
        SK_Payment BIGINT,
        SK_Rate BIGINT,
        SK_Trip BIGINT,
        SK_Flag BIGINT,
        lpep_pickup_datetime TIMESTAMP,
        lpep_dropoff_datetime TIMESTAMP,
        Passenger_count DOUBLE,
        Trip_distance DOUBLE,
        PULocationID BIGINT,
        DOLocationID BIGINT,
        Fare_amount DOUBLE,
        Extra DOUBLE,
        MTA_tax DOUBLE, 
        Improvement_surcharge DOUBLE, 
        Tip_amount DOUBLE, 
        Tolls_amount DOUBLE, 
        Total_amount DOUBLE, 
        congestion_surcharge DOUBLE
        )
    STORED AS PARQUET
    LOCATION 'hdfs://localhost:9000/user/jovianaditya/df_0'
'''

spark.sql(create_dim1)
spark.sql(create_dim2)
spark.sql(create_dim3)
spark.sql(create_dim4)
spark.sql(create_dim5)
spark.sql(create_fact)
