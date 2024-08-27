{{ config(
    materialized='table',
    schema='main',
    name='nova_tabela'
) }}

SELECT * FROM {{ ref('bancos_clean') }}