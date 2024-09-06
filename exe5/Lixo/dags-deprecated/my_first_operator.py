from airflow.decorators import dag, task
from datetime import datetime

@dag(
    start_date=datetime(2024, 9, 3),
    schedule="@daily",
    catchup=False,
    tags=["banco", "reclamacoes"],
)
def banco_reclamacoes():
    @task
    def task1():
        return 1

    @task
    def task2():
        return 2

    @task
    def task3():
        return 3

    task1() >> task2() >> task3()