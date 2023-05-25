from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Remote Hive Metastore Connection") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.sql("select * from green_taxi")

# Convert column lpep_pickup_datetime & lpep_dropoff_datetime into datetime format
from pyspark.sql.functions import col, from_unixtime
df = df.withColumn("lpep_pickup_datetime", from_unixtime(col("lpep_pickup_datetime") / 1000000).cast("timestamp"))
df = df.withColumn("lpep_dropoff_datetime", from_unixtime(col("lpep_dropoff_datetime") / 1000000).cast("timestamp"))
df = df.dropna(subset=["Payment_type"])
df = df.filter(col("RateCodeID") != 99.0)
df = df.drop("ehail_fee")

# Making dimension1
from pyspark.sql.functions import when
df = df.withColumn("VendorName", when(col("VendorID") == 1, "Creative Mobile Technologies, LLC") \
                                 .when(col("VendorID") == 2, "VeriFone Inc") \
                                 .otherwise("Unknown"))
df = df.withColumn("SK_Vendor", when(col("VendorName") == "Creative Mobile Technologies, LLC", 1)
                                       .when(col("VendorName") == "VeriFone Inc" , 2)
                                       .otherwise(3))

# Making dimension2
from pyspark.sql.functions import isnull
df = df.withColumn("PaymentMethod", when(col("Payment_type") == 1, "Credit card") \
                                        .when(col("Payment_type") == 2, "Cash") \
                                        .when(col("Payment_type") == 3, "No charge") \
                                        .when(col("Payment_type") == 4, "Dispute") \
                                        .otherwise("Unknown"))
df = df.withColumn("SK_Payment", when(col("PaymentMethod") == "Credit card", 1) \
                                          .when(col("PaymentMethod") == "Cash" , 2) \
                                          .when(col("PaymentMethod") == "No charge", 3) \
                                          .when(col("PaymentMethod") == "Dispute", 4) \
                                          .otherwise(5))

# Making dimension3
df = df.withColumn("Rate_Desc", when(col("RateCodeID") == 1, "Standard rate") \
                                    .when(col("RateCodeID") == 2, "JFK") \
                                    .when(col("RateCodeID") == 3, "Newark") \
                                    .when(col("RateCodeID") == 4, "Nassau or Westchester") \
                                    .when(col("RateCodeID") == 5, "Negotiated fare") \
                                    .otherwise("Group ride"))
df = df.withColumn("SK_Rate", when(col("Rate_Desc") == "Standard rate", 1) \
                                        .when(col("Rate_Desc") == "JFK" , 2) \
                                        .when(col("Rate_Desc") == "Newark", 3) \
                                        .when(col("Rate_Desc") == "Nassau or Westchester", 4) \
                                        .when(col("Rate_Desc") == "Negotiated fare", 5) \
                                       .otherwise(6))

# Making dimension4
df = df.withColumn("Trip_Desc", when(col("Trip_type") == 1, "Street-hail") \
                                    .otherwise("Dispatch"))
df = df.withColumn("SK_Trip", when(col("Trip_Desc") == "Street-hail", 1) \
                                       .otherwise(2))

# Making dimension5
df = df.withColumn("Flag_Desc", when(col("Store_and_fwd_flag") == "Y", "store and forward trip") \
                                    .otherwise("not a store and forward trip"))
df = df.withColumn("SK_Flag", when(col("Flag_Desc") == "store and forward trip", 1) \
                                       .otherwise(2))

# Seperate dim & fact
df_dim1 = df.selectExpr("SK_Vendor","VendorID", "VendorName").distinct().orderBy("SK_Vendor")
df_dim2 = df.selectExpr("SK_Payment","Payment_type", "PaymentMethod").distinct().orderBy("SK_Payment")
df_dim3 = df.selectExpr("SK_Rate","RateCodeID", "Rate_Desc").distinct().orderBy("SK_Rate")
df_dim4 = df.selectExpr("SK_Trip","Trip_type","Trip_Desc").distinct().orderBy("SK_Trip")
df_dim5 = df.selectExpr("SK_Flag", "Store_and_fwd_flag", "Flag_Desc").distinct().orderBy("SK_Flag")
df_fact = df.selectExpr("SK_Vendor", "SK_Payment", "SK_Rate", "SK_Trip", "SK_Flag", "lpep_pickup_datetime", "lpep_dropoff_datetime",
                         "Passenger_count", "Trip_distance", "PULocationID", "DOLocationID", "Fare_amount", "Extra",
                         "MTA_tax", "Improvement_surcharge", "Tip_amount", "Tolls_amount", "Total_amount", "congestion_surcharge")
df_dim1.show()
df_dim2.show()
df_dim3.show()
df_dim4.show()
df_dim5.show()
df_fact.show()

df_list = [df_fact, df_dim1, df_dim2, df_dim3, df_dim4, df_dim5]

for i, data_frame in enumerate(df_list):
    file_name = f"df_{i}"
    data_frame.coalesce(1).write.parquet(file_name, mode='overwrite')
    print(f"DataFrame {i} converted to {file_name}")
