from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, upper
import os
import mysql.connector
import re


spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)  

def read_and_merge_csv_files(directory_path):

    settings = {
        "Bancos": {"sep": "\t", "message": " Lendo arquivos: {directory_path} -- Filepath {file_path}"},
        "Empregados": {"sep": "|", "message": " Lendo arquivos: {directory_path}"},
        "Reclamacoes": {"sep": ";", "message": " Lendo arquivos: {directory_path}"},
        "default": {"sep": ";", "message": " Lendo arquivos: {directory_path}"}
    }

    setting = settings.get(os.path.basename(directory_path), settings["default"])

    csv_files = [f for f in os.listdir(directory_path) if f.endswith(('.csv', '.tsv'))]

    if not csv_files:
        print(f"No CSV files found in {directory_path}")
        return spark.createDataFrame([], schema=None)

    merged_df = None
    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        try:
            print(setting["message"].format(directory_path=directory_path, file_path=file_path))
            df = spark.read.options(sep=setting["sep"], header=True, encoding="ISO-8859-1").csv(file_path)
            print(f"Arquivo Processado: {file_path}")
            
            if merged_df is None:
                merged_df = df
            else:
                merged_df = merged_df.unionByName(df, allowMissingColumns=True)
        except Exception as file_err:
            print(f"Error reading file {file_path}: {file_err}")

    return merged_df if merged_df is not None else spark.createDataFrame([], schema=None)

def create_raw_layer():
    path = 'Dados/raw'
    os.makedirs(path, exist_ok=True)
    directories_paths = ['Bancos', 'Empregados', 'Reclamacoes']

    for directory in directories_paths:
        print(f" Info: Creating RAW Layer - PATH: {path} Diretorio: {directory}")
        os.makedirs(f'{path}/{directory}', exist_ok=True)
        merged_dataframe = read_and_merge_csv_files(directory)
        if merged_dataframe.rdd.isEmpty():
            print(f"sem dados")
        else:
            output_path = f'{path}/{directory}/output.parquet'
            merged_dataframe.write.parquet(output_path, mode='overwrite')
            
            line_count = spark.read.parquet(output_path).count()


if __name__ == "__main__":
    try:
        print('Running raw layer...')
        create_raw_layer()
        print('Finished script')
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")