-- models/datamart/dm_commodities.sql
WITH commodities AS (
    SELECT
        datahora,
        ticker,
        valor_fechamento
    FROM
        {{ ref ('stg_commodities') }}
),
movimentacao AS (
    SELECT
        datahora,
        ticker,
        acao,
        quantidade
    FROM
        {{ ref ('stg_movimentacao_commodities') }}
),
joined AS (
    SELECT
        C.datahora,
        C.ticker,
        C.valor_fechamento,
        m.acao,
        m.quantidade,
        (
            m.quantidade * C.valor_fechamento
        ) AS valor,
        CASE
            WHEN m.acao = 'sell' THEN (
                m.quantidade * C.valor_fechamento
            )
            ELSE -(
                m.quantidade * C.valor_fechamento
            )
        END AS ganho
    FROM
        commodities C
        INNER JOIN movimentacao m
        ON C.datahora = m.datahora
        AND C.ticker = m.ticker
),
LAST_DAY AS (
    SELECT
        MAX(datahora) AS max_date
    FROM
        joined
),
filtered AS (
    SELECT
        *
    FROM
        joined
    WHERE
        datahora = (
            SELECT
                max_date
            FROM
                LAST_DAY
        )
)
SELECT
    datahora,
    ticker,
    valor_fechamento,
    acao,
    quantidade,
    valor,
    ganho
FROM
    filtered
