import os
import pandas as pd
import numpy as np

def clean_string(df, field):
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
    df["campo_limpo"] = df[field].str.replace(pattern, '', regex=True)
    df["campo_limpo"] = df["campo_limpo"].str.upper()
    df.replace(np.nan, '', inplace=True)
    return df

def create_trusted_layer():
    path = '/app/Dados/trusted'
    os.makedirs(path, exist_ok=True)
    
    directories_paths = ['Bancos', 'Empregados', 'Reclamacoes']
    field_to_clean_dict = {
        "Bancos": "Nome",
        "Empregados": "employer_name",
        "Reclamacoes": "Instituição financeira"
    }
    
    for directory in directories_paths:
        os.makedirs(f'{path}/{directory}', exist_ok=True)
        raw_file_path = f'/app/Dados/raw/{directory}/output.csv'
        
        # Leitura dos dados do diretório raw
        dataframe = pd.read_csv(raw_file_path, sep=';', encoding='latin-1')
        
        # Limpeza de string no campo especificado
        dataframe = clean_string(dataframe, field_to_clean_dict[directory])
        
        # Conversão dos dados para string
        dataframe = dataframe.astype(str)
        
        # Gravação dos dados em formato Parquet no diretório trusted
        trusted_file_path = f'{path}/{directory}/output.parquet'
        dataframe.to_parquet(trusted_file_path)

if __name__ == "__main__":
    print('Running trusted layer...')
    create_trusted_layer()
    print('Trusted layer completed.')
