{{ config(
    materialized='table',
    name='tb_bancos',
    database='mysql'
) }}

SELECT * FROM {{ ref('find') }}