from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

import os

spark = SparkSession.builder.appName("Transactions Ingestion").getOrCreate()

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

date = os.environ.get("PROCESS_DATE")

input_path = f"/opt/airflow/data/landing/transactions_{date}.csv"
output_path = f"/opt/airflow/data/converted_raw/transactions/"

df = spark.read.option("header", True).csv(input_path)

df = df.dropDuplicates(["transaction_id"])

df = df.withColumn("process_date", lit(date))

df.write \
  .mode("overwrite") \
  .partitionBy("process_date") \
  .parquet(output_path)

print("✅ Transactions parquet written")