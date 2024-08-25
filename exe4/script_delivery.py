import duckdb

# Connect to the DuckDB database
duck_con = duckdb.connect(database=':memory:')

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
        SELECT * FROM Bancos
        JOIN Reclamacoes ON Bancos.campo_limpo = Reclamacoes.campo_limpo
        JOIN Empregados ON Bancos.campo_limpo = Empregados.campo_limpo
    """
    duck_con.execute(query)
    duck_con.execute("""COPY
        (SELECT * FROM bancos_reclamacoes_empregados)
        TO './datawarehouse/seeds/bancos_reclamacoes_empregados.parquet'
        (FORMAT 'parquet');"""
    )


if __name__ == "__main__":
    try:
        print('Running delivery layer...')
        join_tables()
        duck_con.close()
        print('Finished script')
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")