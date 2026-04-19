from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Transactions Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

input_path = "/opt/airflow/data/bronze/transactions"
output_path = "/opt/airflow/data/silver/transactions"

df = spark.read.format("delta").load(input_path)

clean_df = (
    df
    .filter(col("transaction_id").isNotNull())
    .filter(col("amount") > 0)
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .withColumn("year", year(col("timestamp")))
    .withColumn("month", month(col("timestamp")))
    .withColumn("day", dayofmonth(col("timestamp")))
    .dropDuplicates(["transaction_id"])
)

clean_df.write.format("delta") \
    .mode("overwrite") \
    .save(output_path)

spark.stop()