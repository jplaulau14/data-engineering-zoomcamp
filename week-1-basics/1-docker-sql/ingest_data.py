import pandas as pd
import argparse
import os
from sqlalchemy import create_engine
from time import time 

def ingest_data(csv_file, table_name, engine, chunk=10000):
    df_iter = pd.read_parquet(csv_file, iterator=True, chunksize=chunk)

    while True:
        try:
            time_start = time()
            df = next(df_iter)
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
            df.to_sql(table_name, engine, if_exists='append', index=False)
            time_end = time()
            print(f'Ingested {chunk} rows in {time_end - time_start} seconds')
        except StopIteration:
            print('Finished ingesting data')
            break

def main(params):
    user = params['user']
    password = params['password']
    host = params['host']
    port = params['port']
    database = params['db']
    table_name = params['table_name']
    url = params['url']
    parquet_file = 'output.parquet'
    os.system(f'wget {url} -O {parquet_file}')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    engine.connect()
    ingest_data(parquet_file, table_name, engine)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data')
    parser.add_argument('--user', required=True, type=str, help='Database user')
    parser.add_argument('--password', required=True, type=str, help='Database password')
    parser.add_argument('--host', required=True, type=str, help='Database host')
    parser.add_argument('--port', required=True, type=str, help='Database port')
    parser.add_argument('--db', required=True, type=str, help='Database name')
    parser.add_argument('--table_name', required=True, type=str, help='Table name')
    parser.add_argument('--url', required=True, type=str, help='URL to download parquet file')
    args = parser.parse_args()
    params = vars(args)
    main(params)