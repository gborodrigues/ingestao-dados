from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, upper
import pyspark.pandas as ps
import os
import mysql.connector
import pandas as pd 
import re

# Configurações de banco de dados
db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_DATABASE')
}

conn = mysql.connector.connect(**db_config)
table_name = "tb_banco_ex3_v3"

# Inicializando o SparkSession
spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .getOrCreate()

spark.conf.set("spark.sql.debug.maxToStringFields", 1000)  # Increase the number as needed

def read_csv_files_in_directory(directory_path):
    files = os.listdir(directory_path)
    csv_files = [file for file in files if file.endswith(('.csv', '.tsv'))]
    dataframes = []
    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
    try:
            if directory_path == "Bancos":

                print(f"    Lendo arquivos: Directory Path {directory_path} -- Filepath {file_path}")
                df = spark.read.options(sep="\t", header=True, encoding="ISO-8859-1").csv(file_path)

                print (f"    Arquivo Processado: {file_path}")

            elif directory_path == "Empregados":

                print(f"    Lendo arquivos: {directory_path}")
                df = spark.read.options(sep="|", header=True, encoding="ISO-8859-1").csv(file_path)

                print (f"    Arquivo Processado: {file_path}")
            else:

                print(f"    Lendo arquivos: {directory_path}")
                df = spark.read.options(sep=";", header=True, encoding="ISO-8859-1").csv(file_path)
                #df.show(10)
                print (f"    Arquivo Processado: {file_path}")


            dataframes.append(df)

    except Exception as file_err:
            print(f"Error reading file {directory_path}: {file_err}")
    if dataframes:
        merged_df = dataframes[0]
        #merged_df = dataframes[0].unionAll(dataframes[1:])
        return merged_df
    else:
        return spark.createDataFrame([], schema=None)

def create_raw_layer():
    path = 'Dados/raw'
    os.makedirs(path, exist_ok=True)
    directories_paths = ['Bancos', 'Empregados', 'Reclamacoes']
    for directory in directories_paths:
        print(f" Info: Creating RAW Layer - PATH: {path} Diretorio: {directory}")
        os.makedirs(f'{path}/{directory}', exist_ok=True)

        dataframe = read_csv_files_in_directory(directory)

        dataframe.write.csv(f'{path}/{directory}/output.csv', sep=';', mode='overwrite', header=True, encoding='latin1')
        print("Dataframe =.....")
        dataframe.show(2)
        dataframe = spark.read \
        .option("header", True) \
        .option("delimiter", ";") \
        .option("encoding", "ISO-8859-1") \
        .option("inferSchema", True) \
        .option("enforceSchema", False) \
        .csv(f'{path}/{directory}/output.csv')


def clean_string(df, field):
    pattern = (
        r' - PRUDENCIAL|'
        r' S\.A[./]?|'
        r' S/A[/]?|'
        r'GRUPO|'
        r' SCFI|'
        r' CC |'
        r' C\.C |'
        r' CCTVM[/]?|'
        r' LTDA[/]?|'
        r' DTVM[/]?|'
        r' BM[/]?|'
        r' CH[/]?|'
        r'COOPERATIVA DE CRÉDITO, POUPANÇA E INVESTIMENTO D[E?O?A/]?|'
        r' [(]conglomerado[)]?|'
        r'GRUPO[ /]|'
        r' -[ /]?'
    )
    df = df.withColumn("campo_limpo", regexp_replace(col(field), pattern, ''))

    df = df.withColumn("campo_limpo", upper(col("campo_limpo")))
    
    return df

def clean_column_name(name):
    if isinstance(name, str):
        name = name.replace(' ', '_').replace('-', '_')
        name = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
    return name

def create_trusted_layer():
    path = 'Dados/trusted'
    os.makedirs(path, exist_ok=True)
    directories_paths = ['Bancos', 'Empregados', 'Reclamacoes']
    field_to_clean_dict = {"Bancos": "Nome", "Empregados": "employer_name", "Reclamacoes": "Instituição financeira"}
    for directory in directories_paths:
        os.makedirs(f'{path}/{directory}', exist_ok=True)
        dataframe = spark.read.csv(f'Dados/raw/{directory}/output.csv', sep=';', header=True, encoding='latin1', inferSchema=True)
        dataframe = clean_string(dataframe, field_to_clean_dict[directory])
        dataframe.write.parquet(f'{path}/{directory}/output.parquet', mode='overwrite')
        print(f" Info: Creating Trusted Layer - PATH: {path} Diretório: {directory}")


def create_table(df):
    cursor = conn.cursor()
    
    # Remover a tabela existente antes de criar uma nova
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    fields = ", ".join([f"{col} VARCHAR(255)" for col in df.columns])
    print(f'{fields}')
    create_table_sql = f"CREATE TABLE {table_name} ({fields});"
    cursor.execute(create_table_sql)
    
    insert_sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"
    
    for row in df.collect():
        cursor.execute(insert_sql, tuple(row.asDict().values()))  
        
    conn.commit()

def insert_data(df):
    cursor = conn.cursor()
    insert_sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"
    
    for row in df.collect():
        cursor.execute(insert_sql, tuple(row.asDict().values()))  
        
    conn.commit()

#Excluir espaço no nome da coluna
def replace_spaces_in_columns(df):
    # Função para ajustar os nomes de colunas conforme as regras do MySQL
    def adjust_column_name(col_name):
        # Substituir espaços e caracteres especiais por underscores
        new_name = re.sub(r'[^0-9a-zA-Z_]', '_', col_name)
        
        # Garantir que o nome não comece com um número
        if re.match(r'^\d', new_name):
            new_name = f'col_{new_name}'
        
        return new_name
    
    # Aplicar a função de ajuste a todos os nomes de colunas
    new_column_names = [adjust_column_name(col_name) for col_name in df.columns]
    
    # Aplicar os novos nomes ao DataFrame
    df = df.toDF(*new_column_names)
    return df

#alterando para funcao union do spark
def delivery_layer():
    path = 'Dados/delivery'
    os.makedirs(path, exist_ok=True)
    directories_paths = ['Bancos', 'Empregados', 'Reclamacoes']
    dataframes = {}
    
    # Leia todos os arquivos parquet e armazene DataFrames em um dicionário
    for directory in directories_paths:
        dataframe = spark.read.parquet(f'Dados/trusted/{directory}/output.parquet')
        dataframes[directory] = dataframe
        print(f" Info: Creating Delivery Layer - PATH: {path} Diretorio: {directory}")
    
    # Join DataFrames
    bancos_df = dataframes['Bancos']
    reclamacoes_df = dataframes['Reclamacoes']
    empregados_df = dataframes['Empregados']
    

    merged_df = bancos_df.join(reclamacoes_df, on=["campo_limpo"], how="inner")
    merged_df.show(2)
    merge_all = merged_df.join(empregados_df, on="campo_limpo", how="inner")
    
    # Lida com conflitos de nomes de colunas
    def rename_columns(df, prefix):
        """Renomeie colunas para evitar conflitos adicionando um prefixo."""
        columns = df.columns
        new_columns = [f"{prefix}_{col}" if col in ['Segmento', 'CNPJ'] else col for col in columns]
        return df.toDF(*new_columns)
    
    # Renomear colunas conflitantes antes da junção final
    bancos_df = rename_columns(bancos_df, 'bancos')
    reclamacoes_df = rename_columns(reclamacoes_df, 'reclamacoes')
    empregados_df = rename_columns(empregados_df, 'empregados')
  
    # Execute join novamente com colunas renomeadas
    merged_df = bancos_df.join(reclamacoes_df, on=["campo_limpo"], how="inner")
  
    common_columns = set(merged_df.columns).intersection(set(empregados_df.columns))
    # Renomear colunas do DataFrame merged_df - aqui o campo_limpo_x
    merged_df_renamed = merged_df.select([col(c).alias(c + "_x") if c in common_columns else col(c) for c in merged_df.columns])

    # Renomear colunas do DataFrame empregados_df
    empregados_df_renamed = empregados_df.select([col(c).alias(c + "_y") if c in common_columns else col(c) for c in empregados_df.columns])

    merge_all = merged_df_renamed.join(empregados_df_renamed, merged_df_renamed.campo_limpo_x == empregados_df_renamed.campo_limpo_y, "inner")
    merge_all.show(2)
    
    # Remove colunas com nomes duplicados (se ainda houver)
    cols_to_drop = [col for col in merge_all.columns if col.endswith('_y')]
    merge_all = merge_all.drop(*cols_to_drop)
    

    # Opcional: renomeie as colunas se necessário
    rename_dict = {
        'bancos_Nome': 'Nome', 
        'Segmento_x': 'Segmento', 
        'CNPJ_x': 'CNPJ'
    }
    for old_name, new_name in rename_dict.items():
        if old_name in merge_all.columns:
            merge_all = merge_all.withColumnRenamed(old_name, new_name)

    # Substituir espaços em nomes de colunas
    merge_all = replace_spaces_in_columns(merge_all)
    
    # Mostrar o esquema final e os dados
    final_columns = merge_all.columns
     
    # Cria tabelas e insere dados
    create_table(merge_all)
    insert_data(merge_all)
    
    # Escreva o DataFrame final no Parquet
    merge_all.write.parquet(f'{path}/dados_finais.parquet', mode='overwrite')

if __name__ == "__main__":
    try:
        print('Running raw layer...')
        create_raw_layer()
        print('Running trusted layer...')
        create_trusted_layer()
        print('Running delivery layer...')
        delivery_layer()
        print('Finished script')
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")
