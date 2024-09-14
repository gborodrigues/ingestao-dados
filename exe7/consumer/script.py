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

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
ec2 = boto3.client('ec2')
rds = boto3.client('rds')


load_dotenv()

db_config = {
    'user':  os.getenv('DB_USER'),
    'password':  os.getenv('DB_PASSWORD'),
    'database':  os.getenv('DB_NAME')
}

table_name = os.getenv('DB_TABLE_NAME')
db_instance_identifier = os.getenv('DB_IDENTIFIER')

def create_security_group():
    try:
        response = ec2.describe_security_groups(Filters=[
            {'Name': 'group-name', 'Values': [os.getenv('SG_NAME')]}
        ])
        
        if response['SecurityGroups']:
            security_group_id = response['SecurityGroups'][0]['GroupId']
            print(f"Security Group already exists: {security_group_id}")
            return security_group_id
        
        response = ec2.create_security_group(
            GroupName=os.getenv('SG_NAME'),
            Description='Security group for MySQL RDS instance'
        )
        security_group_id = response['GroupId']
        
        ec2.authorize_security_group_ingress(
            GroupId=security_group_id,
            IpPermissions=[
                {
                    'IpProtocol': 'tcp',
                    'FromPort': 3306,
                    'ToPort': 3306,
                    'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
                }
            ]
        )
        print(f"Security Group created and configured: {security_group_id}")
        return security_group_id
    
    except Exception as e:
        print(f"Error creating security group: {e}")
        raise

def create_rds_instance(security_group_id):
    try:
        response = rds.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
        
        # Check if the response contains DBInstances
        if 'DBInstances' not in response:
            raise KeyError("Response does not contain 'DBInstances' key")

        if response['DBInstances']:
            instance = response['DBInstances'][0]
            # Ensure 'Endpoint' key exists in the instance
            if 'Endpoint' not in instance:
                raise KeyError("Instance does not contain 'Endpoint' key")

            endpoint = instance['Endpoint']['Address']
            port = instance['Endpoint']['Port']
            print(f"Using existing RDS instance: {db_instance_identifier}")
            print(f"RDS instance endpoint: {endpoint}:{port}")
            return endpoint, port

        response = rds.create_db_instance(
            DBInstanceIdentifier=db_instance_identifier,
            AllocatedStorage=20,
            DBName=db_config['database'],
            Engine='mysql',
            MasterUsername=db_config['user'],
            MasterUserPassword=db_config['password'],
            DBInstanceClass='db.t3.micro',
            VpcSecurityGroupIds=[security_group_id],
            AvailabilityZone='us-east-1a',
            MultiAZ=False,
            PubliclyAccessible=True,
            StorageType='gp2'
        )
        print(f"RDS instance creation initiated: {response['DBInstance']['DBInstanceIdentifier']}")
        
        waiter = rds.get_waiter('db_instance_available')
        waiter.wait(DBInstanceIdentifier=db_instance_identifier)
        
        instance_info = rds.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
        
        # Ensure 'DBInstances' and 'Endpoint' keys exist
        if 'DBInstances' not in instance_info or len(instance_info['DBInstances']) == 0:
            raise KeyError("Instance information is missing or empty")
        
        instance = instance_info['DBInstances'][0]
        if 'Endpoint' not in instance:
            raise KeyError("Instance does not contain 'Endpoint' key")

        endpoint = instance['Endpoint']['Address']
        port = instance['Endpoint']['Port']
        print(f"RDS instance endpoint: {endpoint}:{port}")
        return endpoint, port
    
    except KeyError as e:
        print(f"KeyError: {e}")
        raise
    except Exception as e:
        print(f"Error handling RDS instance: {e}")
        raise



def read_csv_from_s3(bucket_name, file_key):
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    content = response['Body'].read().decode('latin-1')
    try:
        df = pd.read_csv(StringIO(content), sep='\t', on_bad_lines='skip')
        return df
    except Exception as e:
        print(f"Error reading CSV from S3: {e}")
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

def create_table(df, table_name, conn):
    cursor = conn.cursor()
    fields = ", ".join([f"{clean_column_name(col)} VARCHAR(255)" for col in df.columns])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({fields});"
    try:
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Table {table_name} created successfully.")
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")

def get_row_count(table_name, conn):
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = cursor.fetchone()[0]
        print(f"Current row count in {table_name}: {row_count}")
        return row_count
    except Exception as e:
        print(f"Error getting row count: {e}")
        return 0

def insert_data(df, table_name, conn):
    try:
        cursor = conn.cursor()
        columns = [clean_column_name(col) for col in df.columns]
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        print(f"Insert SQL: {insert_sql}")
        for idx, row in enumerate(df.itertuples(index=False, name=None)):
            row = tuple(None if pd.isna(x) else x for x in row)
            print(f"Inserting row {idx + 1}: {row}")
            cursor.execute(insert_sql, row)
        conn.commit()
        print(f"Data inserted successfully into {table_name}. {len(df)} rows inserted.")
    except Exception as e:
        print(f"Error during data insertion: {e}")

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
    try:
        entries = [{'Id': str(i), 'ReceiptHandle': handle} for i, handle in enumerate(receipt_handles)]
        response = sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries)
        if 'Failed' in response and response['Failed']:
            print(f"Failed to delete {len(response['Failed'])} messages from SQS.")
            for failure in response['Failed']:
                print(f"Failed to delete message: {failure}")
        else:
            print(f"Deleted {len(receipt_handles)} messages from SQS.")
    except Exception as e:
        print(f"Error deleting messages from SQS: {e}")

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
    while True:
        messages = read_messages_from_sqs(queue_url, batch_size)
        if not messages:
            print("No more messages in SQS queue.")
            break

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

        print(f"Number of rows to be added to the Parquet file: {len(merged_df)}")
        print(f"First few rows of the DataFrame:\n{merged_df.head()}")

        if parquet_buffer.getvalue():
            existing_df = pd.read_parquet(io.BytesIO(parquet_buffer.getvalue()))
            merged_df = pd.concat([existing_df, merged_df], ignore_index=True)
        
        parquet_buffer = io.BytesIO()
        merged_df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        s3.put_object(Bucket=output_bucket, Key=output_key, Body=parquet_buffer.getvalue())
        print("Data processing completed successfully.")

        receipt_handles = [msg['ReceiptHandle'] for msg in messages]
        delete_messages_from_sqs(queue_url, receipt_handles)

        remaining_messages_response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        remaining_messages = int(remaining_messages_response['Attributes'].get('ApproximateNumberOfMessages', 0))
        print(f"Number of messages remaining in the SQS queue: {remaining_messages}")

def main(s3_bucket, bancos_file_key, sqs_queue_url, output_bucket, output_key):
    # Create security group and RDS instance
    security_group_id = create_security_group()
    endpoint, port = create_rds_instance(security_group_id)
    
    # Update DB config with new RDS endpoint
    db_config['host'] = endpoint
    
    conn = mysql.connector.connect(**db_config)
    engine = create_engine(f"mysql+mysqlconnector://{db_config['user']}:{db_config['password']}@{db_config['host']}:{port}/{db_config['database']}")
    
    bancos_df = read_csv_from_s3(s3_bucket, bancos_file_key)
    
    if bancos_df is not None and 'Nome' in bancos_df.columns:
        bancos_df = clean_string(bancos_df, "Nome")
    else:
        print("Warning: 'Nome' column not found. Skipping string cleaning for Bancos data.")

    row_count = get_row_count(table_name, conn)
    if row_count != 1474:
        if bancos_df is not None:
            print(f"Columns in the DataFrame: {bancos_df.columns.tolist()}")
            create_table(bancos_df, table_name, conn)
            insert_data(bancos_df, table_name, conn)
    else:
        print("Table already contains 1474 rows. Skipping table creation and data insertion.")

    process_batch_and_delete(sqs_queue_url, output_bucket, output_key, batch_size=10)

if __name__ == "__main__":
    try:
        S3_BUCKET = os.getenv('BUCKET_NAME')
        BANCOS_FILE_KEY = 'Bancos/EnquadramentoInicia_v2.csv'
        SQS_QUEUE_URL = os.getenv('SQS_URL')
        OUTPUT_BUCKET = os.getenv('OUTPUT_BUCKET_NAME')
        OUTPUT_KEY = os.getenv('OUTPUT_FILE_NAME')
        main(S3_BUCKET, BANCOS_FILE_KEY, SQS_QUEUE_URL, OUTPUT_BUCKET, OUTPUT_KEY)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
