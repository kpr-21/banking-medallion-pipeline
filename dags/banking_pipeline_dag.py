from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.file_utils import move_processed_data
import os

# -------- PATH CONFIG --------
BASE_DIR = "/opt/airflow/data/landing"

# -------- DEFAULT ARGS --------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 4, 14),
    "retries": 3
}

# -------- DAG --------
with DAG(
    dag_id="banking_pipeline",
    default_args=default_args,
    schedule_interval=None,  # manual trigger
    catchup=False
) as dag:

    # -----------------------------
    # FILE SENSORS
    # -----------------------------
    wait_for_customers = FileSensor(
        task_id="wait_for_customers",
        filepath=f"{BASE_DIR}/customers_{{{{ ds }}}}.csv",
        poke_interval=10,
        timeout=300,
        mode="poke"
    )

    wait_for_accounts = FileSensor(
        task_id="wait_for_accounts",
        filepath=f"{BASE_DIR}/accounts_{{{{ ds }}}}.csv",
        poke_interval=10,
        timeout=300,
        mode="poke"
    )

    wait_for_transactions = FileSensor(
        task_id="wait_for_transactions",
        filepath=f"{BASE_DIR}/transactions_{{{{ ds }}}}.csv",
        poke_interval=10,
        timeout=300,
        mode="poke"
    )

    # -----------------------------
    # DATA GENERATION (OPTIONAL)
    # -----------------------------
    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python /opt/airflow/scripts/data_generator.py {{ ds }}"
    )

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

    move_customers = PythonOperator(
    task_id="move_customers",
    python_callable=move_processed_data,
    op_kwargs={
        "table_name": "customers",
        "process_date": "{{ ds }}"
    }
    )

    move_accounts = PythonOperator(
    task_id="move_accounts",
    python_callable=move_processed_data,
    op_kwargs={
        "table_name": "accounts",
        "process_date": "{{ ds }}"
    }
    )

    move_transactions = PythonOperator(
    task_id="move_transactions",
    python_callable=move_processed_data,
    op_kwargs={
        "table_name": "transactions",
        "process_date": "{{ ds }}"
    }
    )
    # -----------------------------
    # TASK ORDER
    # -----------------------------
    generate_data >> [
    wait_for_customers,
    wait_for_accounts,
    wait_for_transactions
    ]

# Step 2
    # Parquet → Bronze
    customers_to_parquet >> customers_to_bronze
    accounts_to_parquet >> accounts_to_bronze
    transactions_to_parquet >> transactions_to_bronze

    # Bronze → Move
    customers_to_bronze >> move_customers
    accounts_to_bronze >> move_accounts
    transactions_to_bronze >> move_transactions
