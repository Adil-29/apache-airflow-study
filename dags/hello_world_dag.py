from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_context(**context):
    print("Logical Date:", context["logical_date"])
    print("Run ID:", context["run_id"])
    print("Task ID:", context["task"].task_id)
    print("DAG ID:", context["dag"].dag_id)
    print("Execution Date (ds):", context["ds"])

with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2026, 7, 20),
    schedule="@hourly",
    catchup=True,
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=print_context,
    )