from airflow import DAG
from datetime import datetime
from datetime import datetime, timedelta

with DAG(
    dag_id="hourly_partitioned_data_gen",
    start_date=datetime.now() - timedelta(hours=24),
    schedule="@hourly",
    catchup=True,
) as dag:
    pass