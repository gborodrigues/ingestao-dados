import os
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
import mysql.connector


DB_NAME = os.getenv('DB_NAME', 'ingestao-dados')

def print_env_vars():
    print(f"DB_HOST: {os.getenv('DB_HOST')}")
    print(f"DB_DATABASE: {os.getenv('DB_DATABASE')}")
    print(f"DB_USER: {os.getenv('DB_USER')}")
    print(f"DB_NAME: {DB_NAME}")

def clean_column_name(name):
    if isinstance(name, str):
        name = name.replace(' ', '_').replace('-', '_')
        name = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
    return name

def create_database(cursor, db_name):
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        print(f"Database '{db_name}' created successfully.")
    except mysql.connector.Error as err:
        print(f"Erro ao criar Bando de Dados: {err}")
        raise

def create_table(cursor, df, table_name):
    fields = ", ".join([f"`{col}` VARCHAR(255)" for col in df.columns])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({fields});"
    cursor.execute(create_table_sql)
    print(f"Tabela '{table_name}' criada com sucesso")

def insert_data(cursor, df, table_name):
    insert_sql = f"INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(df.columns))})"
    for row in df.itertuples(index=False, name=None):
        cursor.execute(insert_sql, row)
    print(f"Dados inseridos na tabela '{table_name}' com sucesso.")

def delivery_layer():
    print("Criando Delivery Layer...")
    print_env_vars()

    path = '/app/Dados/delivery'
    os.makedirs(path, exist_ok=True)
    
    directories_paths = ['Bancos', 'Empregados', 'Reclamacoes']
    dataframes = {}
    
    for directory in directories_paths:
        dataframe = pd.read_parquet(f'/app/Dados/trusted/{directory}/output.parquet', engine='pyarrow')
        dataframes[directory] = dataframe

    merged_df = pd.merge(dataframes['Bancos'], dataframes['Reclamacoes'], on=["campo_limpo"])
    merged_all = pd.merge(merged_df, dataframes['Empregados'], on="campo_limpo")

    merged_all.columns = [clean_column_name(col) for col in merged_all.columns]
    merged_all = merged_all.drop(['Nome_y', 'Segmento_y', 'CNPJ_y', 'Unnamed__14'], axis=1, errors='ignore')
    merged_all.columns = merged_all.columns.str.replace('_x', '')

    try:
        mysql_hook = MySqlHook(mysql_conn_id='exe5-db-1')
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()

        # Create Database
        create_database(cursor, DB_NAME)

        # Alterando paro novo Banco de Dados
        cursor.execute(f"USE `{DB_NAME}`")

        table_name = "tb_banco"
        create_table(cursor, merged_all, table_name)
        insert_data(cursor, merged_all, table_name)

        conn.commit()
        print("Todas as operações no Banco de Dados foram executadas com sucesso.")
    except mysql.connector.Error as err:
        print(f"Erro na operacarao do Banco de Dados: {err}")
        print(f"Parametros de Conexao: {mysql_hook.get_connection('exe5-db-1').get_uri()}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        print("MySQL Fechado Conexao.")

    merged_all.to_parquet(f'{path}/dados_finais.parquet')
    print("Delivery layer Finalizada.")

if __name__ == "__main__":
    delivery_layer()