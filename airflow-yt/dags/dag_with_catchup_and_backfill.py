from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "gabriel",
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_with_catchup_and_backfill_v2",
    default_args=default_args,
    description="Our first DAG",
    start_date=datetime(2024, 8, 30),
    schedule_interval="@daily",
    catchup=False,  # Campo para não rodar a dag com a data de inicio para a data de termino
) as dag:
    task1 = BashOperator(
        task_id="task1",
        bash_command="echo Simple bash command",
    )

    task1


# airflow dags backfill -s 2024-08-01 -e 2024-09-30 dag_with_catchup_and_backfill_v2 --> rodar no cli para rodar
