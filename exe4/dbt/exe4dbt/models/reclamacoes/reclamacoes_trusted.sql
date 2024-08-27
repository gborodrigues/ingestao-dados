{{ config(
    materialized='table',
    schema='main',
    name='nova_tabela'
) }}

SELECT * FROM {{ ref('reclamacoes_clean') }}