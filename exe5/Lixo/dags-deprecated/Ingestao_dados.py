from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# Diretório onde seus scripts estão localizados
scripts_dir = os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'dags', 'scripts')

# Definindo a DAG
with DAG(
    'pipeline_sequencial',  # Nome da DAG
    default_args={'owner': 'airflow', 'start_date': datetime(2024, 9, 6)},  # Configurações padrão
    schedule_interval=None,  # Sem agendamento automático
    catchup=False,  # Não faz catchup de execuções passadas
) as dag:

    # Task para executar o script raw.py
    raw_task = BashOperator(
        task_id='process_raw',
        bash_command=f'python {os.path.join(scripts_dir, "raw_layer.py")}',
    )

    # Task para executar o script trusted.py
    trusted_task = BashOperator(
        task_id='process_trusted',
        bash_command=f'python {os.path.join(scripts_dir, "trusted_layer.py")}',
    )

    # Task para executar o script delivery.py
    delivery_task = BashOperator(
        task_id='process_delivery',
        bash_command=f'python {os.path.join(scripts_dir, "delivery_layer.py")}',
    )

    # Definindo a sequência de execução
    raw_task >> trusted_task >> delivery_task
