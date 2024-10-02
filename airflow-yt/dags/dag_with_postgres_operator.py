from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "gabriel",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_with_postgres_operator_v1",
    default_args=default_args,
    start_date=datetime(2024, 9, 30),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = PostgresOperator(
        task_id="task1",
        postgres_conn_id="postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS dag_runs (
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            );
        """,
    )

    task1
