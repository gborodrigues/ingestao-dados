from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

with DAG('delivery_layer_dag', start_date=datetime(2023, 9, 1), schedule_interval=None) as dag:
    wait_for_trusted = ExternalTaskSensor(
        task_id='wait_for_trusted_layer',
        external_dag_id='trusted_layer_dag',
        external_task_id='run_trusted_layer',
        mode='poke',
        timeout=600
    )

    run_delivery = BashOperator(
        task_id='run_delivery_layer',
        bash_command='python /opt/airflow/dags/scripts/delivery_layer.py'
    )

    wait_for_trusted >> run_delivery  # Define a dependência
