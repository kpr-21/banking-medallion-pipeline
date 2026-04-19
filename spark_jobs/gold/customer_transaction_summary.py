from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Gold - Customer Transaction Summary") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

customers_path = "/opt/airflow/data/silver/customers"
transactions_path = "/opt/airflow/data/silver/transactions"
output_path = "/opt/airflow/data/gold/customer_transaction_summary"

customers_df = spark.read.format("delta").load(customers_path)
transactions_df = spark.read.format("delta").load(transactions_path)

gold_df = (
    transactions_df.groupBy("customer_id")
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum("amount").alias("total_transaction_amount"),
        avg("amount").alias("avg_transaction_amount"),
        max("amount").alias("max_transaction_amount")
    )
    .join(
        customers_df.select("customer_id", "first_name", "last_name", "email", "full_name"),
        on="customer_id",
        how="left"
    )
)

gold_df.write.format("delta") \
    .mode("overwrite") \
    .save(output_path)

spark.stop()