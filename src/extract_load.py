import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
load_dotenv()

DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

commodities = ["CL=F", "GC=F", "HG=F", "SI=F", "ZC=F"]

def buscar_dados_commodities(tickers, periodo='5y', intervalo='1d'):
    ticker = yf.Ticker(tickers)
    dados =ticker.history(period=periodo, interval=intervalo)[['Close']]
    dados['Ticker'] = tickers
    return dados

def buscar_todos_dados_commodities():
    todos_dados = []
    for ticker in commodities:
        dados = buscar_dados_commodities(ticker)
        todos_dados.append(dados)
    return pd.concat(todos_dados)

def salvar_dados(df, schema='public'):
    df.to_sql('commodities', engine, if_exists='replace', index=True, index_label='Date', schema=schema)


if __name__ == "__main__":
    dados_concatenados = buscar_todos_dados_commodities()
    salvar_dados(dados_concatenados)
    