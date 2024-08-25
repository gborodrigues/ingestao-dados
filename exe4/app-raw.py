from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, upper
import os
import mysql.connector
import re

# Inicializando o SparkSession

spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .getOrCreate()

spark.conf.set("spark.sql.debug.maxToStringFields", 1000)  # Increase the number as needed

def read_csv_files_in_directory(directory_path):
    files = os.listdir(directory_path)
    csv_files = [file for file in files if file.endswith(('.csv', '.tsv'))]
    dataframes = []

    # Definicao de configs para cada diretorio
    settings = {
        "Bancos": {"sep": "\t", "message": "    Lendo arquivos: {directory_path} -- Filepath {file_path}"},
        "Empregados": {"sep": "|", "message": "    Lendo arquivos: {directory_path}"},
        "Reclamacoes": {"sep": ";", "message": "    Lendo arquivos: {directory_path}"},
        "default": {"sep": ";", "message": "    Lendo arquivos: {directory_path}"}
    }

    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        
        # Para uso em melhoria futura - Caso nao esteja no dicionario (settings), usar config default
        setting = settings.get(directory_path, settings["default"])

        try:
            print(setting["message"].format(directory_path=directory_path, file_path=file_path))
            df = spark.read.options(sep=setting["sep"], header=True, encoding="ISO-8859-1").csv(file_path)
            print(f"Arquivo Processado: {file_path}")
            dataframes.append(df)

        except Exception as file_err:
            print(f"Error reading file {directory_path}: {file_err}")

    if dataframes:
        merged_df = dataframes[0]
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

        dataframe.write.parquet(f'{path}/{directory}/output.parquet', mode='overwrite')


if __name__ == "__main__":
    try:
        print('Running raw layer...')
        create_raw_layer()
        print('Finished script')
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")
