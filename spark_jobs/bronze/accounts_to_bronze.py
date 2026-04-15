import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from delta import configure_spark_with_delta_pip
# -------- SPARK --------

builder = SparkSession.builder \
    .appName("accounts bronze") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
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