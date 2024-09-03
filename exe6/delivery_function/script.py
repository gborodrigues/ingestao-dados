import os
from boto3 import client, resource
import pandas as pd
import pymysql
from botocore.exceptions import ClientError
import io

# AWS S3 configuration
s3 = client('s3')
bucket_name = os.getenv('BUCKET_NAME')
s3_resource = resource('s3')

prefix = 'trusted/'

rds_port = 3306
rds_user = 'root'
rds_password = 'root1234567'
rds_db_name = 'data_analysis'
table_name = 'delivery'
db_instance_identifier = 'delivery-g5-97909'
security_group_id = 'sg-05b79d50f713b483d'

def create_rds_instance_if_not_exists():
    rds = client('rds')
    try:
        response = rds.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
        print("RDS instance already exists.")
        return response['DBInstances'][0]['Endpoint']['Address']
    except ClientError as e:
        if e.response['Error']['Code'] == 'DBInstanceNotFound':
            print("Creating new RDS instance...")
            rds.create_db_instance(
                DBName=rds_db_name,
                DBInstanceIdentifier=db_instance_identifier,
                AllocatedStorage=20,
                DBInstanceClass='db.t3.micro',
                Engine='mysql',
                MasterUsername=rds_user,
                MasterUserPassword=rds_password,
                VpcSecurityGroupIds=[security_group_id],
                PubliclyAccessible=True
            )
            waiter = rds.get_waiter('db_instance_available')
            waiter.wait(DBInstanceIdentifier=db_instance_identifier)
            print("RDS instance created successfully.")
            
            response = rds.describe_db_instances(DBInstanceIdentifier=db_instance_identifier)
            return response['DBInstances'][0]['Endpoint']['Address']
        else:
            raise

def get_connection(host):
    return pymysql.connect(
        host=host,
        user=rds_user,
        password=rds_password,
        database=rds_db_name,
        port=rds_port
    )

def clean_column_name(col):
    return ''.join(e for e in col if e.isalnum() or e == '_').lower()

def create_table(df, conn):
    with conn.cursor() as cursor:
        fields = ", ".join([f"{clean_column_name(col)} VARCHAR(255)" for col in df.columns])
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({fields});"
        cursor.execute(create_table_sql)
    conn.commit()

def insert_data(df, conn):
    with conn.cursor() as cursor:
        columns = [clean_column_name(col) for col in df.columns]
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        values = [tuple(row) for row in df.values]
        cursor.executemany(insert_sql, values)
    conn.commit()

def read_parquet_from_s3(bucket, key):
    obj = s3_resource.Object(bucket, key)
    with io.BytesIO(obj.get()['Body'].read()) as buf:
        return pd.read_parquet(buf)


def upload_parquet_to_s3(df, bucket, key):
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine='pyarrow')
    buffer.seek(0)
    s3_resource.Object(bucket, key).put(Body=buffer.getvalue())


def delivery_layer():
    directories_paths = ['Bancos', 'Empregados', 'Reclamacoes']
    dataframes = {}

    for directory in directories_paths:
        key = f"{prefix}{directory}/output.parquet"
        try:
            dataframes[directory] = read_parquet_from_s3(bucket_name, key)
        except Exception as e:
            print(f"Error reading {key}: {str(e)}")
            raise

    merged_df = pd.merge(dataframes['Bancos'], dataframes['Reclamacoes'], on="campo_limpo")
    merge_all = pd.merge(merged_df, dataframes['Empregados'], on="campo_limpo")

    merge_all.columns = [clean_column_name(col) for col in merge_all.columns]
    merge_all = merge_all.drop(['nome_y', 'segmento_y', 'cnpj_y', 'unnamed_14'], axis=1, errors='ignore')
    merge_all.columns = merge_all.columns.str.replace('_x', '')

    final_parquet_path = 'delivery/merged_data.parquet'
    upload_parquet_to_s3(merge_all, bucket_name, final_parquet_path)
    
    print(f"Uploaded merged data to S3 at {final_parquet_path}")

    try:
        rds_host = create_rds_instance_if_not_exists()
        print(f"Connecting to RDS at: {rds_host}")
        conn = get_connection(rds_host)
        print("Successfully connected to RDS")

        try:
            create_table(merge_all, conn)
            insert_data(merge_all, conn)
            print("Data successfully inserted into RDS")
        finally:
            conn.close()
    except Exception as e:
        print(f"Error during RDS operations: {str(e)}")
        raise

def handler(event, _):  
    try:
        print('Running delivery layer...')
        delivery_layer()
        print('Finished script')
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()