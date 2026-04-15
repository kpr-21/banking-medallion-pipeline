import os
import shutil

BASE_PATH = "/opt/airflow/data"

def move_processed_data(table_name, process_date):
    src = f"{BASE_PATH}/converted_raw/{table_name}/process_date={process_date}"
    dest = f"{BASE_PATH}/processed_raw/{table_name}/process_date={process_date}"

    if os.path.exists(src):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        shutil.move(src, dest)
        print(f"✅ Moved {table_name} data for {process_date}")
    else:
        print(f"⚠️ No data found for {table_name} on {process_date}")