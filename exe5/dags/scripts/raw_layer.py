import os
import pandas as pd

def read_csv_files_in_directory(directory_path):
    print(f"Executando funcao Read CSV files in directory - Directory Path = {directory_path}")
    files = os.listdir(directory_path)
    csv_files = [file for file in files if file.endswith(('.csv', '.tsv'))]
    dataframes = []
    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        try:
            if "Bancos" in directory_path:
                df = pd.read_csv(file_path, sep='\t', encoding='latin-1')
            elif "Empregados" in directory_path:
                df = pd.read_csv(file_path, sep='|', encoding='latin-1')
            else:
                df = pd.read_csv(file_path, sep=';', encoding='latin-1')
            dataframes.append(df)
        except pd.errors.EmptyDataError:
            print(f"Skipping empty file: {file_path}")
        except Exception as file_err:
            raise (f"Error reading file {file_path}: {file_err}")
    merged_df = pd.concat(dataframes, ignore_index=True)
    return merged_df

def create_raw_layer():
    raw_path = '/app/Dados/raw/'
    os.makedirs(raw_path, exist_ok=True)
    source_directories = {
        'Bancos': '/app/Bancos',
        'Empregados': '/app/Empregados',
        'Reclamacoes': '/app/Reclamacoes'
    }
    
    for directory, source_path in source_directories.items():
        print(f'Executando 1 {source_path} - {directory}')
        os.makedirs(f'{raw_path}/{directory}', exist_ok=True)
        dataframe = read_csv_files_in_directory(source_path)
        dataframe.to_csv(f'{raw_path}/{directory}/output.csv', sep=';', index=False, encoding='latin-1')
        
    print("Raw layer criada com sucesso")
    
if __name__ == "__main__":
    print('Iniciando a criação da camada raw...')
    create_raw_layer()
    print('Criação da camada raw concluída.')
