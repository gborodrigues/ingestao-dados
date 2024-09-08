from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Connection
from airflow.utils.db import provide_session

# Função para adicionar a conexão MySQL
@provide_session
def add_mysql_connection(session=None):
    # Detalhes da conexão
    conn_id = 'exe5-db-1'
    conn_type = 'mysql'
    conn_host = 'exe5-db-1' #Nome do container do Mysql no Docker Compose
    conn_schema = 'ingestao-dados'
    conn_login = 'ingestao'
    conn_password = 'ingestao'
    conn_port = 3306
    print(conn_login)
    print(conn_password)

    # Verifica se a conexão já existe
    existing_conn = session.query(Connection).filter_by(conn_id=conn_id).first()
    if not existing_conn:
        # Cria a nova conexão
        new_conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=conn_host,
            schema=conn_schema,
            login=conn_login,
            password=conn_password,
            port=conn_port
        )
        print(new_conn)
        # Adiciona e confirma a conexão no banco de dados do Airflow
        session.add(new_conn)
        session.commit()
        print(f"Conexão '{conn_id}' criada com sucesso.")
    else:
        print(f"A conexão '{conn_id}' já existe.")

# Definir a DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1
}

with DAG(
    'create_mysql_connection_dag',
    default_args=default_args,
    description='DAG para criar conexão MySQL no Airflow',
    schedule_interval=None,  # Executa manualmente
    catchup=False
) as dag:

    # Operador Python para criar a conexão MySQL
    create_mysql_connection_task = PythonOperator(
        task_id='create_mysql_connection',
        python_callable=add_mysql_connection
    )

    create_mysql_connection_task
