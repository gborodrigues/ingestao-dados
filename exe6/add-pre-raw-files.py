import boto3
import os
from botocore.exceptions import ClientError

def create_bucket(s3_client, bucket_name):
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        print(f"Erro ao criar bucket t: {e}")
        return False
    return True

def upload_folder(s3_client, bucket_name, folder_path, s3_folder):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, folder_path)
            s3_path = os.path.join(s3_folder, relative_path)
            
            print(f"Uploading {local_path} to {s3_path}")
            s3_client.upload_file(local_path, bucket_name, s3_path)

def main():

    bucket_name = "dados-exe6-v1"
    
    s3_client = boto3.client('s3')

    if create_bucket(s3_client, bucket_name):
        print(f"Bucket '{bucket_name}' created or already exists. Maybe both?")
    else:
        print(f"Failed to create bucket '{bucket_name}'. Exiting, my man hehe")
        return

    parent_folder = 'pre-raw-data'

    subfolders = ['Bancos', 'Empregados', 'Reclamacoes']
    
    for subfolder in subfolders:
        local_folder_path = os.path.join(os.getcwd(), parent_folder, subfolder)
        if os.path.exists(local_folder_path):
            upload_folder(s3_client, bucket_name, local_folder_path, subfolder)
        else:
            print(f"Folder {subfolders} not found :P")

if __name__ == "__main__":
    main()