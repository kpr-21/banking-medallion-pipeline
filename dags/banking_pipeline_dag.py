from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from utils.file_utils import move_processed_data

# -------- PATH CONFIG --------
BASE_DIR = "/opt/airflow/data/landing"

# -------- DEFAULT ARGS --------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 14),
    "retries": 2
}

with DAG(
    dag_id="banking_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # -----------------------------
    # FILE SENSORS
    # -----------------------------
    wait_for_customers = FileSensor(
        task_id="wait_for_customers",
        filepath=f"{BASE_DIR}/customers_{{{{ ds }}}}.csv",
        poke_interval=10,
        timeout=300
    )

    wait_for_accounts = FileSensor(
        task_id="wait_for_accounts",
        filepath=f"{BASE_DIR}/accounts_{{{{ ds }}}}.csv",
        poke_interval=10,
        timeout=300
    )

    wait_for_transactions = FileSensor(
        task_id="wait_for_transactions",
        filepath=f"{BASE_DIR}/transactions_{{{{ ds }}}}.csv",
        poke_interval=10,
        timeout=300
    )

    # -----------------------------
    # GENERATE DATA
    # -----------------------------
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python /opt/airflow/scripts/data_generator.py {{ ds }}"
    )

    # -----------------------------
    # INGESTION (CSV → PARQUET)
    # -----------------------------
    customers_to_parquet = BashOperator(
        task_id="customers_to_parquet",
        bash_command="PROCESS_DATE={{ ds }} python /opt/airflow/spark_jobs/ingestion/customers_to_parquet.py"
    )

    accounts_to_parquet = BashOperator(
        task_id="accounts_to_parquet",
        bash_command="PROCESS_DATE={{ ds }} python /opt/airflow/spark_jobs/ingestion/accounts_to_parquet.py"
    )

    transactions_to_parquet = BashOperator(
        task_id="transactions_to_parquet",
        bash_command="PROCESS_DATE={{ ds }} python /opt/airflow/spark_jobs/ingestion/transactions_to_parquet.py"
    )

    # -----------------------------
    # BRONZE
    # -----------------------------
    customers_to_bronze = BashOperator(
        task_id="customers_to_bronze",
        bash_command="python /opt/airflow/spark_jobs/bronze/customers_to_bronze.py"
    )

    accounts_to_bronze = BashOperator(
        task_id="accounts_to_bronze",
        bash_command="python /opt/airflow/spark_jobs/bronze/accounts_to_bronze.py"
    )

    transactions_to_bronze = BashOperator(
        task_id="transactions_to_bronze",
        bash_command="python /opt/airflow/spark_jobs/bronze/transactions_to_bronze.py"
    )

    # -----------------------------
    # MOVE (Landing → Archive)
    # -----------------------------
    move_customers = PythonOperator(
        task_id="move_customers",
        python_callable=move_processed_data,
        op_kwargs={"table_name": "customers", "process_date": "{{ ds }}"}
    )

    move_accounts = PythonOperator(
        task_id="move_accounts",
        python_callable=move_processed_data,
        op_kwargs={"table_name": "accounts", "process_date": "{{ ds }}"}
    )

    move_transactions = PythonOperator(
        task_id="move_transactions",
        python_callable=move_processed_data,
        op_kwargs={"table_name": "transactions", "process_date": "{{ ds }}"}
    )

    # -----------------------------
    # SILVER (SparkSubmit FIXED)
    # -----------------------------
    # -----------------------------
# SILVER (SparkSubmit FIXED)
# -----------------------------
    customers_to_silver = SparkSubmitOperator(
    task_id="customers_to_silver",
    application="/opt/airflow/spark_jobs/silver/customers_to_silver.py",
    conn_id=None,
    name="customers_silver_job",
    verbose=True,
    conf={
        "spark.master": "local[*]",  # ✅ FIXED HERE
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    },
    packages="io.delta:delta-spark_2.12:3.1.0"
    )

    accounts_to_silver = SparkSubmitOperator(
    task_id="accounts_to_silver",
    application="/opt/airflow/spark_jobs/silver/accounts_to_silver.py",
    conn_id=None,
    name="accounts_silver_job",
    verbose=True,
    conf={
        "spark.master": "local[*]",  # ✅ FIXED HERE
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    },
    packages="io.delta:delta-spark_2.12:3.1.0"
    )

    transactions_to_silver = SparkSubmitOperator(
    task_id="transactions_to_silver",
    application="/opt/airflow/spark_jobs/silver/transactions_to_silver.py",
    conn_id=None,
    name="transactions_silver_job",
    verbose=True,
    conf={
        "spark.master": "local[*]",  # ✅ FIXED HERE
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    },
    packages="io.delta:delta-spark_2.12:3.1.0"
    )
    gold_customer_transaction_summary = SparkSubmitOperator(
    task_id="gold_customer_transaction_summary",
    application="/opt/airflow/spark_jobs/gold/customer_transaction_summary.py",
    conn_id=None,
    name="gold_customer_transaction_summary_job",
    verbose=True,
    conf={
        "spark.master": "local[*]",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    },
    packages="io.delta:delta-spark_2.12:3.1.0"
    )

    gold_account_balance_summary = SparkSubmitOperator(
        task_id="gold_account_balance_summary",
        application="/opt/airflow/spark_jobs/gold/account_balance_summary.py",
        conn_id=None,
        name="gold_account_balance_summary_job",
        verbose=True,
        conf={
            "spark.master": "local[*]",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        },
        packages="io.delta:delta-spark_2.12:3.1.0"
    )

    gold_transaction_type_summary = SparkSubmitOperator(
        task_id="gold_transaction_type_summary",
        application="/opt/airflow/spark_jobs/gold/transaction_type_summary.py",
        conn_id=None,
        name="gold_transaction_type_summary_job",
        verbose=True,
        conf={
            "spark.master": "local[*]",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        },
        packages="io.delta:delta-spark_2.12:3.1.0"
    )

    gold_customer_account_360 = SparkSubmitOperator(
        task_id="gold_customer_account_360",
        application="/opt/airflow/spark_jobs/gold/customer_account_360.py",
        conn_id=None,
        name="gold_customer_account_360_job",
        verbose=True,
        conf={
            "spark.master": "local[*]",
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        },
        packages="io.delta:delta-spark_2.12:3.1.0"
    )

    # -----------------------------
    # PIPELINE FLOW (CORRECT)
    # -----------------------------

    generate_data >> [
        wait_for_customers,
        wait_for_accounts,
        wait_for_transactions
    ]

    # Ingestion
    wait_for_customers >> customers_to_parquet >> customers_to_bronze >> move_customers >> customers_to_silver
    wait_for_accounts >> accounts_to_parquet >> accounts_to_bronze >> move_accounts >> accounts_to_silver
    wait_for_transactions >> transactions_to_parquet >> transactions_to_bronze >> move_transactions >> transactions_to_silver

    customers_to_silver >> [
    gold_customer_transaction_summary,
    gold_customer_account_360
    ]

    accounts_to_silver >> [
        gold_account_balance_summary,
        gold_customer_account_360
    ]

    transactions_to_silver >> [
        gold_transaction_type_summary,
        gold_customer_transaction_summary,
        gold_customer_account_360
    ]