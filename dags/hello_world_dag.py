from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello from Apache Airflow! 🚀")
    print("My first DAG is working successfully.")

with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "learning"],
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=print_hello,
    )

    hello_task