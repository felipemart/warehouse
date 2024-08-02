WITH source AS (
    SELECT
        DATE AS datahora,
        ticker,
        action,
        quantity
    FROM
        {{ source(
            'warehouse',
            'movimentacao_commodities'
        ) }}
),
renamed AS (
    SELECT
        CAST(
            datahora AS DATE
        ) AS datahora,
        ticker AS ticker,
        action AS acao,
        quantity AS quantidade
    FROM
        source
)
SELECT
    *
FROM
    renamed
