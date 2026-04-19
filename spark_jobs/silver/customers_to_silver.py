from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, concat_ws, when
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Customers Silver") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

input_path = "/opt/airflow/data/bronze/customers"
output_path = "/opt/airflow/data/silver/customers"

df = spark.read.format("delta").load(input_path)

clean_df = (
    df
    .filter(col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
    .withColumn(
        "email_domain",
        when(col("email").contains("@"), split(col("email"), "@")[1]).otherwise(None)
    )
)

clean_df.write.format("delta") \
    .mode("overwrite") \
    .save(output_path)

spark.stop()