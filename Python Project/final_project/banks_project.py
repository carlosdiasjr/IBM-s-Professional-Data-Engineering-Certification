import warnings
warnings.filterwarnings('ignore')
#################

import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

PATH = '/Users/carlosdias/Documents/IBM Data Engineering/Python Project/final_project'
URL = 'https://en.wikipedia.org/wiki/List_of_largest_banks'

def log_progress(message):
    log = f'{datetime.now()} - {message}\n'
    print(log)
    with open(f'{PATH}/code_log.txt', 'a') as file:
        file.write(log)

def extract(url):
    log_progress('Starting table extraction.')
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.findAll('table', {'class': 'wikitable'})
        df = pd.read_html(str(tables[0]))[0]
        log_progress('Table extraction sucessful.')
        return df
    else:
        log_progress('Table extraction failed.')
        return None

def transform(df):
    log_progress('Starting data transformation.')
    exchange_rates_df = pd.read_csv(f'{PATH}/exchange_rate.csv')
    exchange_rates = dict(zip(exchange_rates_df['Currency'], exchange_rates_df['Rate']))

    for currency, rate in exchange_rates.items():
        df[f'Market cap ({currency} billion)'] = df['Market cap (US$ billion)'] * rate
        df[f'Market cap ({currency} billion)'] = df[f'Market cap ({currency} billion)'].round(2)

    log_progress('Data transformation completed.')
    return df

def load_to_csv(df, filename=f'{PATH}/output.csv'):
    log_progress('Starting loading data to CSV.')
    df.to_csv(filename, index=False)
    log_progress('Data loaded to CSV.')

def load_to_db(df, db_name=f'{PATH}/database.db', table_name='banks'):
    log_progress('Starting loading data to database.')
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    log_progress('Data loaded to database.')


def run_queries(db_name=f'{PATH}/database.db'):
    query = 'SELECT * FROM banks LIMIT 5'
    log_progress(f'Starting database query: {query}')
    conn = sqlite3.connect(db_name)
    result = pd.read_sql_query(query, conn)
    conn.close()
    log_progress('Queries executed.')
    return result

dataframe = extract(URL)

print(dataframe)

transformed_df = transform(dataframe)

print(transformed_df)
print(transformed_df.to_string())

load_to_csv(transformed_df)

load_to_db(transformed_df)

query_results = run_queries()
print(query_results)



