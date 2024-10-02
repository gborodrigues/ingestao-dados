import pandas as pd
import os
import json
from confluent_kafka import Producer

def read_csv_files_in_directory(directory_path):
    dataframes = []
    for filename in os.listdir(directory_path):
        if filename.endswith(('.csv', '.tsv')):
            file_path = os.path.join(directory_path, filename)
            try:
                df = pd.read_csv(file_path, sep=';', encoding='latin-1')
                dataframes.append(df)
            except pd.errors.EmptyDataError:
                print(f"Skipping empty file: {file_path}")
            except Exception as file_err:
                raise Exception(f"Error reading file {file_path}: {file_err}")

    if dataframes:
        merged_df = pd.concat(dataframes, ignore_index=True)
        return merged_df
    else:
        return pd.DataFrame()

def read_files():
    directory = 'Reclamacoes2/'
    dataframe = read_csv_files_in_directory(directory)
    return dataframe

def send_messages_to_kafka(df, topic):
    conf = {
        'bootstrap.servers': 'localhost:9092',
    }
    producer = Producer(**conf)

    for index, row in df.iterrows():
        message = row.to_dict()
        message_body = json.dumps(message)
        
        try:
            producer.produce(topic, value=message_body)
            producer.poll(0)
        except Exception as e:
            print(f"Failed to send message: {message_body}")
            print(e)

    producer.flush()

def handler():
    try:
        print('Reading files from local directory...')
        dataframe = read_files()
        if not dataframe.empty:
            topic = 'gestao'
            print('Sending messages to Kafka...')
            send_messages_to_kafka(dataframe, topic)
            print('Done!')
        else:
            print('No data to send.')
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == '__main__':
    handler()
