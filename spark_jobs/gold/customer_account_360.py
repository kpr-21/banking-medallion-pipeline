from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Gold - Customer Account 360") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

customers_path = "/opt/airflow/data/silver/customers"
accounts_path = "/opt/airflow/data/silver/accounts"
transactions_path = "/opt/airflow/data/silver/transactions"
output_path = "/opt/airflow/data/gold/customer_account_360"

customers_df = spark.read.format("delta").load(customers_path)
accounts_df = spark.read.format("delta").load(accounts_path)
transactions_df = spark.read.format("delta").load(transactions_path)

account_agg_df = (
    accounts_df.groupBy("customer_id")
    .agg(
        count("account_id").alias("total_accounts"),
        sum("balance").alias("total_balance"),
        avg("balance").alias("avg_balance")
    )
)

transaction_agg_df = (
    transactions_df.groupBy("customer_id")
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum("amount").alias("total_transaction_amount"),
        avg("amount").alias("avg_transaction_amount")
    )
)

gold_df = (
    customers_df
    .join(account_agg_df, on="customer_id", how="left")
    .join(transaction_agg_df, on="customer_id", how="left")
)

gold_df.write.format("delta") \
    .mode("overwrite") \
    .save(output_path)

spark.stop()