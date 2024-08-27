import duckdb
import os

# Connect to the DuckDB database
duck_con = duckdb.connect(database='database.duckdb')

def join_tables():
    dataframes = {}
    directories_paths = ['Bancos', 'Empregados', 'Reclamacoes']
    for directory in directories_paths:
        dataframe = duck_con.read_parquet(f'Dados/trusted/{directory}/output.parquet/*.parquet')
        dataframes[directory] = dataframe
        query = f"""
            CREATE OR REPLACE TABLE {directory} AS
            SELECT * FROM dataframe
        """
        duck_con.execute(query)

    query = f"""
        CREATE OR REPLACE TABLE bancos_reclamacoes_empregados AS
        SELECT * FROM bancos_clean
        JOIN reclamacoes_clean ON bancos_clean.campo_limpo = reclamacoes_clean.campo_limpo
        JOIN empregados_clean ON bancos_clean.campo_limpo = empregados_clean.campo_limpo
    """
    duck_con.execute(query)
    duck_con.execute("""COPY
        (SELECT * FROM bancos_reclamacoes_empregados)
        TO './Dados/delivery/bancos_reclamacoes_empregados.parquet'
        (FORMAT 'parquet');"""
    )


if __name__ == "__main__":
    try:
        print('Running delivery layer...')
        path = 'Dados/delivery'
        os.makedirs(path, exist_ok=True)
        join_tables()
        duck_con.close()
        print('Finished script')
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")