from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('raw_layer_dag', start_date=datetime(2023, 9, 1), schedule_interval=None) as dag:
    run_raw = BashOperator(
        task_id='run_raw_layer',
        bash_command='python /opt/airflow/dags/scripts/raw_layer.py'
    )
