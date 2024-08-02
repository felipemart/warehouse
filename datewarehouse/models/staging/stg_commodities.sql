-- import
WITH source AS (
    SELECT
        "Date" AS datahora,
        "Close",
        "Ticker" AS ticker
    FROM
        {{ source (
            'warehouse',
            'commodities'
        ) }}
),
renamed AS (
    SELECT
        CAST(
            datahora AS DATE
        ) AS datahora,
        "Close" AS valor_fechamento,
        ticker
    FROM
        source
)
SELECT
    *
FROM
    renamed
