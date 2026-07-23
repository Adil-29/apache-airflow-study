from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import csv
import random

BASE_PATH = "/opt/airflow/airflow_data"


def create_execution_folder(**context):
    logical_date = context["logical_date"]
    folder_name = logical_date.strftime("data=%Y-%m-%d-%H")
    folder_path = os.path.join(BASE_PATH, folder_name)
    os.makedirs(folder_path, exist_ok=True)

    print(f"Folder Created: {folder_path}")


def generate_csv_files(**context):
    logical_date = context["logical_date"]
    folder_name = logical_date.strftime("data=%Y-%m-%d-%H")
    folder_path = os.path.join(BASE_PATH, folder_name)

    os.makedirs(folder_path, exist_ok=True)

    num_files = random.randint(5, 10)

    event_types = ["click", "purchase", "login", "logout"]

    for file_num in range(1, num_files + 1):
        file_name = f"part_{file_num:04d}.csv"
        file_path = os.path.join(folder_path, file_name)

        num_rows = random.randint(50, 200)

        with open(file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)

            writer.writerow(
                ["id", "timestamp", "user_id", "event_type", "value"]
            )

            for row_id in range(1, num_rows + 1):
                random_minute = random.randint(0, 59)
                random_second = random.randint(0, 59)

                timestamp = logical_date.replace(
                    minute=random_minute,
                    second=random_second,
                )

                writer.writerow(
                    [
                        row_id,
                        timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                        random.randint(1000, 9999),
                        random.choice(event_types),
                        round(random.uniform(10, 1000), 2),
                    ]
                )

        print(f"Created: {file_path}")


with DAG(
    dag_id="hourly_partitioned_data_gen",
    start_date=datetime.now() - timedelta(hours=24),
    schedule="@hourly",
    catchup=True,
    tags=["assignment"],
) as dag:

    create_folder_task = PythonOperator(
        task_id="create_execution_folder",
        python_callable=create_execution_folder,
    )

    generate_csv_task = PythonOperator(
        task_id="generate_csv_files",
        python_callable=generate_csv_files,
    )

    create_folder_task >> generate_csv_task