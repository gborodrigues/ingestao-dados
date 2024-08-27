{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * 
    FROM {{ source('main', 'bancos') }}
),

cleaned_data AS (
    SELECT 
        *,
        UPPER(
            REGEXP_REPLACE(
                Nome,
                ' - PRUDENCIAL| S\.A[./]?| S/A[/]?|GRUPO| SCFI| CC | C\.C | CCTVM[/]?| LTDA[/]?| DTVM[/]?| BM[/]?| CH[/]?|COOPERATIVA DE CRÉDITO, POUPANÇA E INVESTIMENTO D[E?O?A/]?| [(]conglomerado[)]?|GRUPO[ /]| -[ /]?',
                '',
                'g'
            )
        ) AS campo_limpo
    FROM source_data
)

SELECT 
    *,
    COALESCE(campo_limpo, '') AS campo_limpo
FROM cleaned_data
