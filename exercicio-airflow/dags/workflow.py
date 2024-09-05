from airflow.decorators import dag, task
from datetime import datetime
from scripts import app
from airflow.operators.python_operator import PythonOperator

@dag(
    start_date=datetime(2024, 9, 1), 
    schedule_interval='@daily', 
    catchup=False,
    tags=['bancos', 'reclamacoes']
)
def reclamacoes_workflow():
    
    executar_script = PythonOperator(
        task_id='executar_script_python',
        python_callable=app.create_raw_layer,
    )

    executar_script


reclamacoes_workflow()