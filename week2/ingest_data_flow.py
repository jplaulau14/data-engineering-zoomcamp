import pandas as pd
import argparse
import os
from sqlalchemy import create_engine
from time import time
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=5, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url, chunksize=100000):
    if not os.path.exists('data'):
        os.mkdir('data')
    if not os.path.exists('data/yellow_tripdata'):
        os.mkdir('data/yellow_tripdata')
    file_name = os.path.basename(url)
    parquet_file = f'data/yellow_tripdata/{file_name}'
    os.system(f'wget {url} -O {parquet_file}')
    df = pd.read_parquet(parquet_file)
    return df

@task(log_prints=True)
def transform_data(df):
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    return df

@task(log_prints=True)
def load_data(df, table_name):
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.to_sql(name=table_name, con=engine, if_exists='append')
    
@flow(name="Ingest Flow")
def main_flow():
    table_name = "yellow_taxi_trips"
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-04.parquet"
    df = extract_data(url)
    df_transformed = transform_data(df)
    load_data(df_transformed, table_name)

if __name__ == '__main__':
    main_flow()