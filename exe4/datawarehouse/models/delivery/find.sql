{{ config(
    materialized='table',
    database='duckdb',
    schema='main'
) }}


SELECT * FROM bancos_reclamacoes_empregados