from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Accounts Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

input_path = "/opt/airflow/data/bronze/accounts"
output_path = "/opt/airflow/data/silver/accounts"

df = spark.read.format("delta").load(input_path)

clean_df = (
    df
    .filter(col("account_id").isNotNull())
    .dropDuplicates(["account_id"])
    .withColumn(
        "account_status",
        when(col("balance") < 0, "OVERDRAWN")
        .otherwise("ACTIVE")
    )
)

clean_df.write.format("delta") \
    .mode("overwrite") \
    .save(output_path)

spark.stop()