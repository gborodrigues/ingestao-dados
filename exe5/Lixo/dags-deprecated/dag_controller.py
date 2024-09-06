from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('controller_dag', default_args=default_args, schedule_interval=None) as dag:
    
    trigger_raw = TriggerDagRunOperator(
        task_id='trigger_raw_layer_dag',
        trigger_dag_id='raw_layer_dag'
    )

    trigger_trusted = TriggerDagRunOperator(
        task_id='trigger_trusted_layer_dag',
        trigger_dag_id='trusted_layer_dag'
    )   

    trigger_delivery = TriggerDagRunOperator(
        task_id='trigger_delivery_layer_dag',
        trigger_dag_id='delivery_layer_dag'
    )

    trigger_raw >> trigger_trusted >> trigger_delivery  # Sequência
