from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    "owner": "gabriel",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="dag_with_taskflow_api",
    default_args=default_args,
    start_date=datetime(2024, 9, 30),
    schedule_interval="@daily",
)
def hello_world_etl():
    @task(multiple_outputs=True)
    def get_name():
        return {
            "first_name": "Gabriel",
            "last_name": "Oliveira",
        }

    @task
    def get_age():
        return 29

    @task
    def greet(first_name, last_name, age):
        print(
            f"Hello World! My name is {first_name} {last_name} and I'm {age} years old"
        )

    name_dict = get_name()
    age = get_age()
    greet(name_dict["first_name"], name_dict["last_name"], age)


greet_dag = hello_world_etl()
