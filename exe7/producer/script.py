import pandas as pd
import boto3
from io import BytesIO
from dotenv import load_dotenv
import os
import json

load_dotenv()

bucket_name = os.getenv('BUCKET_NAME')
queue_url =  os.getenv('SQS_URL')

s3_client = boto3.client('s3')
sqs = boto3.client('sqs')


def read_csv_files_in_s3(directory_path):
    print(bucket_name)
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=directory_path)
    dataframes = []
    for obj in response.get('Contents', []):
        if obj['Key'].endswith(('.csv', '.tsv')):
            file_key = obj['Key']
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            file_content = s3_object['Body'].read()
            try:
                df = pd.read_csv(BytesIO(file_content), sep=';', encoding='latin-1')
                dataframes.append(df)
            except pd.errors.EmptyDataError:
                print(f"Skipping empty file: {file_key}")
            except Exception as file_err:
                raise Exception(f"Error reading file {file_key}: {file_err}")
    merged_df = pd.concat(dataframes, ignore_index=True)
    return merged_df

def read_files():
    directory = 'Reclamacoes/'
    dataframe = read_csv_files_in_s3(directory)
    return dataframe

def send_message_batch(df):
    messages = []
    for index, row in df.iterrows():
        message = row.to_dict()
        messages.append(message)

    entries = []
    for i, message in enumerate(messages):
        if isinstance(message, dict):
            message_body = json.dumps(message)
        else:
            message_body = message

        entries.append({
            'Id': str(i),
            'MessageBody': message_body
        })
    
    for i in range(0, len(entries), 10):
        batch = entries[i:i+10]
        try:
            response = sqs.send_message_batch(
                QueueUrl=queue_url,
                Entries=batch
            )
            print(f"{i} files sent successfully")
        except Exception as e:
            print(f"Failed to send batch: {batch}")
            print(e)

def handler(event, _):  
    try:
        print('Reading files from s3...')
        dataframe = read_files()
        print('Sending messages...')
        send_message_batch(dataframe)
        print('Done!')
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")
