import io
import pandas as pd
import numpy as np
import mysql.connector
import boto3
import json
from io import StringIO
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.DEBUG)

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
rds = boto3.client('rds')

load_dotenv()

db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'host': 'mydb-instance.cbl0obhtfsdl.us-east-1.rds.amazonaws.com'
}

table_name = os.getenv('DB_TABLE_NAME')
db_instance_identifier = os.getenv('DB_IDENTIFIER')

def read_csv_from_s3(bucket_name, file_key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('latin-1')
        df = pd.read_csv(StringIO(content), sep='\t', on_bad_lines='skip')
        return df
    except Exception as e:
        logging.error(f"Error reading CSV from S3: {e}")
        return None

def clean_string(df, field):
    if field not in df.columns:
        return df

    df[field] = df[field].astype(str)
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
    df["campo_limpo"] = df[field].str.replace(pattern, '', regex=True).str.upper()
    df.replace(np.nan, '', inplace=True)
    return df

def clean_column_name(name):
    if isinstance(name, str):
        name = name.replace(' ', '_').replace('-', '_')
        name = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
    return name

def read_messages_from_sqs(queue_url, batch_size=10):
    messages = []
    while len(messages) < batch_size:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=min(10, batch_size - len(messages)),
            WaitTimeSeconds=10
        )
        if 'Messages' in response:
            messages.extend(response['Messages'])
        else:
            break
    return messages

def delete_messages_from_sqs(queue_url, receipt_handles):
    """
    Deletes a batch of messages from an SQS queue after processing.
    """
    try:
        entries = [{'Id': str(i), 'ReceiptHandle': handle} for i, handle in enumerate(receipt_handles)]
        response = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
        if 'Failed' in response and response['Failed']:
            logging.error(f"Failed to delete {len(response['Failed'])} messages from SQS.")
            for failure in response['Failed']:
                logging.error(f"Failed to delete message: {failure}")
        else:
            logging.info(f"Deleted {len(receipt_handles)} messages from SQS.")
    except Exception as e:
        logging.error(f"Error deleting messages from SQS: {e}")

def process_sqs_messages(messages):
    data = []
    for message in messages:
        body = json.loads(message['Body'])
        for key in body:
            if body[key] == 'NaN':
                body[key] = None
        data.append(body)
    return pd.DataFrame(data)

def process_batch_and_delete(queue_url, output_bucket, output_key, batch_size=10):

    parquet_buffer = io.BytesIO()

    messages = read_messages_from_sqs(queue_url, batch_size)
    if not messages:
        logging.info("No more messages in SQS queue.")

    reclamacoes_df = process_sqs_messages(messages)
    
    if 'Instituição financeira' in reclamacoes_df.columns:
        reclamacoes_df = clean_string(reclamacoes_df, "Instituição financeira")

    response = rds.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
    instance = response['DBInstances'][0]
    endpoint = instance['Endpoint']['Address']
    port = instance['Endpoint']['Port']

    engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{endpoint}:{port}/{db_config['database']}")
    bancos_from_db_df = pd.read_sql(f"SELECT * FROM {table_name};", engine)

    if 'campo_limpo' in bancos_from_db_df.columns and 'campo_limpo' in reclamacoes_df.columns:
        merged_df = pd.merge(bancos_from_db_df, reclamacoes_df, on="campo_limpo")
    else:
        merged_df = bancos_from_db_df.merge(reclamacoes_df)

    merged_df.columns = [clean_column_name(col) for col in merged_df.columns]
    columns_to_drop = [col for col in merged_df.columns if col.endswith('_y') or col == 'Unnamed__14']
    merged_df = merged_df.drop(columns=columns_to_drop, axis=1, errors='ignore')
    merged_df.columns = merged_df.columns.str.replace('_x', '')

    for col in merged_df.columns:
        merged_df[col] = merged_df[col].astype(str)

    logging.info(f"Number of rows to be added to the Parquet file: {len(merged_df)}")

    if parquet_buffer.getvalue():
        existing_df = pd.read_parquet(io.BytesIO(parquet_buffer.getvalue()))
        merged_df = pd.concat([existing_df, merged_df], ignore_index=True)
    
    parquet_buffer = io.BytesIO()
    merged_df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    s3.put_object(Bucket=output_bucket, Key=output_key, Body=parquet_buffer.getvalue())
    logging.info("Data processing completed successfully.")

    receipt_handles = [msg['ReceiptHandle'] for msg in messages]
    delete_messages_from_sqs(queue_url, receipt_handles)

    remaining_messages_response = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    remaining_messages = int(remaining_messages_response['Attributes'].get('ApproximateNumberOfMessages', 0))
    logging.info(f"Number of messages remaining in the SQS queue: {remaining_messages}")

def main(s3_bucket, bancos_file_key, sqs_queue_url, output_bucket, output_key):
    logging.info("Starting main process")
    
    try:
        logging.info("Database connection established.")

        bancos_df = read_csv_from_s3(s3_bucket, bancos_file_key)
        
        if bancos_df is not None and 'Nome' in bancos_df.columns:
            bancos_df = clean_string(bancos_df, "Nome")
        else:
            logging.warning("Warning: 'Nome' column not found. Skipping string cleaning for Bancos data.")

        process_batch_and_delete(sqs_queue_url, output_bucket, output_key, batch_size=10)
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

def process_sqs_message(message_body, s3_bucket, bancos_file_key, output_bucket, output_key):
    try:
        SQS_QUEUE_URL = os.getenv('SQS_URL')
        main(s3_bucket, bancos_file_key, SQS_QUEUE_URL, output_bucket, output_key)
    except Exception as e:
        logging.error(f"Error processing message: {message_body}. Error: {e}")

def handler(event, _):
    try:
        logging.info(f"SQS event: {json.dumps(event)}")

        S3_BUCKET = os.getenv('BUCKET_NAME')
        BANCOS_FILE_KEY = 'Bancos/EnquadramentoInicia_v2.csv'
        OUTPUT_BUCKET = os.getenv('OUTPUT_BUCKET_NAME')
        OUTPUT_KEY = os.getenv('OUTPUT_FILE_NAME')

        for sqs_record in event['Records']:
            message_body = json.loads(sqs_record['body'])

            process_sqs_message(message_body, S3_BUCKET, BANCOS_FILE_KEY, OUTPUT_BUCKET, OUTPUT_KEY)

        logging.info("SQS messages processed successfully :P")
    
    except Exception as e:
        logging.error(f"An error occurred while processing the SQS event: {e}")
        raise e
