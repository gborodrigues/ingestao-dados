import duckdb
import glob

# Defina os diretórios onde os arquivos Parquet estão localizados
# directories = {
#     'bancos': '/app/Dados/raw/Bancos/output.parquet/*.parquet',
#     'empregados': '/app/Dados/raw/Empregados/output.parquet/*.parquet',
#     'reclamacoes': '/app/Dados/raw/Reclamacoes/output.parquet/*.parquet'
# }

directories = {
    'bancos': '../exe4/Dados/raw/Bancos/output.parquet/*.parquet',
    'empregados': '../exe4/Dados/raw/Empregados/output.parquet/*.parquet',
    'reclamacoes': '../exe4/Dados/raw/Reclamacoes/output.parquet/*.parquet'
}


# Crie uma conexão DuckDB
conn = duckdb.connect('victor.duckdb')

# Função para ler todos os arquivos Parquet em um diretório e combiná-los em uma única tabela
def load_and_combine_parquet_files(directory_pattern, table_name):
    # Encontrar todos os arquivos Parquet no diretório especificado
    files = glob.glob(directory_pattern)
    
    # Combinar todos os arquivos em uma única tabela
    if files:
        print(f"Carregando arquivos {files} como {table_name}")
        # Carregar e combinar todos os arquivos Parquet em uma única tabela
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM parquet_scan({files})")
        print(f"Tabela criada: {table_name}")
    else:
        print(f"Nenhum arquivo encontrado em {directory_pattern}")

# Carregar e combinar arquivos Parquet para cada diretório
for table_name, pattern in directories.items():
    load_and_combine_parquet_files(pattern, table_name)

# Verificar tabelas criadas
tables = conn.execute("SHOW TABLES").fetchall()
print("Tabelas disponíveis:", tables)

# Realizar o join entre as tabelas usando o campo 'campo_limpo'
# Comentado para focar no SELECT das tabelas individuais
# expected_tables = ['bancos', 'empregados', 'reclamacoes']
# if all(table in [t[0] for t in tables] for table in expected_tables):
#     query = """
#         SELECT *
#         FROM bancos
#         JOIN empregados ON bancos.campo_limpo = empregados.campo_limpo
#         JOIN reclamacoes ON bancos.campo_limpo = reclamacoes.campo_limpo
#     """
#     # Execute a consulta e carregue o resultado em um DataFrame
#     df = conn.execute(query).df()
#     # Exibir as primeiras linhas do DataFrame resultante
#     print(df.head())
# else:
#     print("Uma ou mais tabelas esperadas não foram encontradas. Verifique os dados.")

# Exibir o conteúdo de cada tabela criada
for table_name in directories.keys():
    print(f"\nConteúdo da tabela '{table_name}':")
    df = conn.execute(f"SELECT * FROM {table_name}").df()
    print(df)


# Mostrar tabelas no formato "schema - tabela"
schema_query = """
SELECT table_schema, table_name
FROM information_schema.tables
ORDER BY table_schema, table_name;
"""
tables_final = conn.execute(schema_query).fetchall()
print(" ")
print("##############################")
print("Tabelas Finais disponíveis:")
for schema, table in tables_final:
    print(f"{schema} - {table}")
print("##############################")
print(" ")


# Fechar a conexão
conn.close()