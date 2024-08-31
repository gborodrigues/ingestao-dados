import pandas as pd
import numpy as np
import os
import boto3
import tempfile

s3_client = boto3.client('s3')
bucket_name = os.getenv('BUCKET_NAME')

def clean_string(df, field):
    if field not in df.columns:
        raise ValueError(f"Column '{field}' is not present in the DataFrame")

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

    df[field] = df[field].fillna('')
    df["campo_limpo"] = df[field].str.replace(pattern, '', regex=True)
    df["campo_limpo"] = df["campo_limpo"].str.upper()
    df.replace(np.nan, '', inplace=True)
    return df

def upload_to_s3(local_file, s3_key):
    s3_client.upload_file(local_file, bucket_name, s3_key)

def create_trusted_layer():
    directories_paths = ['Bancos', 'Empregados', 'Reclamacoes']
    field_to_clean_dict = {
        "Bancos": "Nome",
        "Empregados": "employer_name",
        "Reclamacoes": "Instituição financeira"
    }

    for directory in directories_paths:

        input_s3_key = f'raw/{directory}/output.csv'
        output_s3_key = f'trusted/{directory}/output.parquet'

        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_csv:
            s3_client.download_file(bucket_name, input_s3_key, temp_csv.name)
            temp_csv.seek(0)
            dataframe = pd.read_csv(temp_csv.name, sep=';', encoding='latin-1')

        dataframe = clean_string(dataframe, field_to_clean_dict[directory])
        dataframe = dataframe.astype(str)

        with tempfile.NamedTemporaryFile(delete=False, suffix='.parquet') as temp_parquet:
            dataframe.to_parquet(temp_parquet.name, index=False)
            temp_parquet.seek(0)
            upload_to_s3(temp_parquet.name, output_s3_key)

        os.remove(temp_csv.name)
        os.remove(temp_parquet.name)

def handler(event, _):  
    try:
        print('Running trusted layer...')
        create_trusted_layer()
        print('Finished script')
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
