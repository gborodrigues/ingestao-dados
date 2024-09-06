from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

# Argumentos padrão para o DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['seu_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

# Criação do DAG
with DAG('meu_primeiro_dag', default_args=default_args, schedule_interval=None) as dag:
    # Tarefa 1: Imprimir a data atual
    task1 = BashOperator(
        task_id='imprimir_data',
        bash_command='date'
    )

    # Tarefa 2: Imprimir uma mensagem personalizada
    task2 = BashOperator(
        task_id='mensagem_personalizada',
        bash_command='echo "Olá, mundo!"'
    )

    # Definindo a dependência: task2 depende de task1
    task1 >> task2