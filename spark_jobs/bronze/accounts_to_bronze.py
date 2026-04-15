import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# -------- SPARK --------
spark = SparkSession.builder \
    .appName("accounts_bronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# -------- PATHS --------
BASE_PATH = "/opt/airflow/data"

input_path = f"{BASE_PATH}/converted_raw/accounts"
output_path = f"{BASE_PATH}/bronze/accounts"

# -------- READ --------
df = spark.read.parquet(input_path)

# -------- METADATA --------
df = df.withColumn("ingestion_timestamp", current_timestamp())

# -------- WRITE --------
df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("process_date") \
    .save(output_path)

print("✅ Accounts Bronze Loaded")