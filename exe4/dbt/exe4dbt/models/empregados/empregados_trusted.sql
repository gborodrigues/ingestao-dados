{{ config(
    materialized='table',
    schema='main',
    name='nova_tabela'
) }}

SELECT * FROM {{ ref('empregados_clean') }}