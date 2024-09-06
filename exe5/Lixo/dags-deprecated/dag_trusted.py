from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

with DAG('trusted_layer_dag', start_date=datetime(2023, 9, 1), schedule_interval=None) as dag:
    wait_for_raw = ExternalTaskSensor(
        task_id='wait_for_raw_layer',
        external_dag_id='raw_layer_dag',
        external_task_id='run_raw_layer',
        mode='poke',  # Pode ser 'reschedule' para eficiência de recursos
        timeout=600  # Timeout opcional
    )

    run_trusted = BashOperator(
        task_id='run_trusted_layer',
        bash_command='python /opt/airflow/dags/scripts/trusted_layer.py'
    )

    wait_for_raw >> run_trusted  # Define a dependência
