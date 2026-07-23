from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from datetime import datetime
import os

def create_execution_folder(**context):
    logical_date = context["logical_date"]
    folder_name = logical_date.strftime("data=%Y-%m-%d-%H")
    base_path = "/tmp/airflow_data"
    folder_path = os.path.join(base_path, folder_name)
    os.makedirs(folder_path, exist_ok=True)
    print(f"Folder Created: {folder_path}")

with DAG(
    dag_id="hourly_partitioned_data_gen",
    start_date=datetime.now() - timedelta(hours=24),
    schedule="@hourly",
    catchup=True,
) as dag:
    create_folder_task = PythonOperator(
        task_id="create_execution_folder",
        python_callable=create_execution_folder,
    )