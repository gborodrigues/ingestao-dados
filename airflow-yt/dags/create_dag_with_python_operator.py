from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "gabriel",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}


def greet(age, ti):
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    print(f"Hello World! My name is {first_name} {last_name} and I'm {age} years old")


def get_name(ti):
    ti.xcom_push(key="first_name", value="Gabriel")
    ti.xcom_push(key="last_name", value="Oliveira")


with DAG(
    dag_id="our_dag_with_python_operator_v3",
    default_args=default_args,
    description="DAG with python",
    start_date=datetime(2024, 9, 30),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greet,
        op_kwargs={"age": 29},
    )

    task2 = PythonOperator(
        task_id="get_name",
        python_callable=get_name,
    )

    task2 >> task1
