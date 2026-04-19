from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, max
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Gold - Transaction Type Summary") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

transactions_path = "/opt/airflow/data/silver/transactions"
output_path = "/opt/airflow/data/gold/transaction_type_summary"

transactions_df = spark.read.format("delta").load(transactions_path)

gold_df = (
    transactions_df.groupBy("transaction_type")
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        max("amount").alias("max_amount")
    )
)

gold_df.write.format("delta") \
    .mode("overwrite") \
    .save(output_path)

spark.stop()