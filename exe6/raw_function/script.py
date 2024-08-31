import pandas as pd
import boto3
from io import BytesIO
import os


# Configuração do S3
s3_client = boto3.client('s3')
bucket_name = os.getenv('BUCKET_NAME')

def read_csv_files_in_s3(directory_path):
    # Lista os objetos no bucket S3 para o diretório especificado
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory_path)
    dataframes = []
    for obj in response.get('Contents', []):
        if obj['Key'].endswith(('.csv', '.tsv')):
            file_key = obj['Key']
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            file_content = s3_object['Body'].read()
            try:
                if directory_path == "Bancos/":
                    df = pd.read_csv(BytesIO(file_content), sep='\t', encoding='latin-1')
                elif directory_path == "Empregados/":
                    df = pd.read_csv(BytesIO(file_content), sep='|', encoding='latin-1')
                else:
                    df = pd.read_csv(BytesIO(file_content), sep=';', encoding='latin-1')
                dataframes.append(df)
            except pd.errors.EmptyDataError:
                print(f"Skipping empty file: {file_key}")
            except Exception as file_err:
                raise Exception(f"Error reading file {file_key}: {file_err}")
    merged_df = pd.concat(dataframes, ignore_index=True)
    return merged_df



def create_raw_layer():
    directories_paths = ['Bancos/', 'Empregados/', 'Reclamacoes/']
    for directory in directories_paths:
        dataframe = read_csv_files_in_s3(directory)
        # Salva o CSV na camada raw dentro do bucket S3
        output_buffer = BytesIO()
        dataframe.to_csv(output_buffer, sep=';', index=False, encoding='latin-1')
        directory_key = directory.rstrip('/')
        s3_client.put_object(Bucket=bucket_name, Key=f'raw/{directory_key}/output.csv', Body=output_buffer.getvalue())

def handler(event, _):  
    try:
        print(event)
        print('Running raw layer...')
        create_raw_layer()
        print('Finished script')
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")