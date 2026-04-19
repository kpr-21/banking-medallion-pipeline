from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg, max, min
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Gold - Account Balance Summary") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

accounts_path = "/opt/airflow/data/silver/accounts"
output_path = "/opt/airflow/data/gold/account_balance_summary"

accounts_df = spark.read.format("delta").load(accounts_path)

gold_df = (
    accounts_df.groupBy("account_type", "account_status")
    .agg(
        count("account_id").alias("total_accounts"),
        sum("balance").alias("total_balance"),
        avg("balance").alias("avg_balance"),
        max("balance").alias("max_balance"),
        min("balance").alias("min_balance")
    )
)

gold_df.write.format("delta") \
    .mode("overwrite") \
    .save(output_path)

spark.stop()