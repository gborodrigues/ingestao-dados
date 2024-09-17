import io
import pandas as pd
import numpy as np
import mysql.connector
import boto3
from io import StringIO
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.DEBUG)

s3 = boto3.client('s3')

load_dotenv()

db_config = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'host': 'database-2.cfs4m4s2cmt7.us-east-1.rds.amazonaws.com'
}

table_name = os.getenv('DB_TABLE_NAME')

def read_csv_from_s3(bucket_name, file_key):
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    content = response['Body'].read().decode('latin-1')
    try:
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

def create_table(df, table_name, conn):
    cursor = conn.cursor()
    fields = ", ".join([f"{clean_column_name(col)} VARCHAR(255)" for col in df.columns])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({fields});"
    try:
        cursor.execute(create_table_sql)
        conn.commit()
        logging.info(f"Table {table_name} created successfully.")
    except mysql.connector.Error as err:
        logging.error(f"Error creating table: {err}")

def get_row_count(table_name, conn):
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        row_count = cursor.fetchone()[0]
        logging.info(f"Current row count in {table_name}: {row_count}")
        return row_count
    except Exception as e:
        logging.error(f"Error getting row count: {e}")
        return 0

def insert_data(df, table_name, conn):
    try:
        cursor = conn.cursor()
        columns = [clean_column_name(col) for col in df.columns]
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
        logging.info(f"Insert SQL: {insert_sql}")
        for idx, row in enumerate(df.itertuples(index=False, name=None)):
            row = tuple(None if pd.isna(x) else x for x in row)
            logging.info(f"Inserting row {idx + 1}: {row}")
            cursor.execute(insert_sql, row)
        conn.commit()
        logging.info(f"Data inserted successfully into {table_name}. {len(df)} rows inserted.")
    except Exception as e:
        logging.error(f"Error during data insertion: {e}")

def main(s3_bucket, bancos_file_key):
    logging.info("Starting main process")
    
    try:
        conn = mysql.connector.connect(**db_config)
        logging.info("Database connection established.")

        bancos_df = read_csv_from_s3(s3_bucket, bancos_file_key)
        
        if bancos_df is not None and 'Nome' in bancos_df.columns:
            bancos_df = clean_string(bancos_df, "Nome")
        else:
            logging.warning("Warning: 'Nome' column not found. Skipping string cleaning for Bancos data.")

        row_count = get_row_count(table_name, conn)
        if row_count != 1474:
            if bancos_df is not None:
                logging.info(f"Columns in the DataFrame: {bancos_df.columns.tolist()}")
                create_table(bancos_df, table_name, conn)
                insert_data(bancos_df, table_name, conn)
        else:
            logging.info("Table already contains 1474 rows. Skipping table creation and data insertion.")

    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        if conn.is_connected():
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":  
    try:
        S3_BUCKET = os.getenv('BUCKET_NAME')
        BANCOS_FILE_KEY = 'Bancos/EnquadramentoInicia_v2.csv'
        main(S3_BUCKET, BANCOS_FILE_KEY)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")