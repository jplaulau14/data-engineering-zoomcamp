import pandas as pd
import os
from sqlalchemy import create_engine
from time import time
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

def parquet_to_csv(parquet_file, csv_file):
    df = pd.read_parquet(parquet_file, engine = 'pyarrow')
    df.to_csv(csv_file, index=False)

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    parquet_file = 'output.parquet'
    csv_file = 'output.csv'
    os.system(f'wget {url} -O {parquet_file}')
    parquet_to_csv(parquet_file, csv_file)
    
    df = pd.read_csv(csv_file)
    return df

@task(log_prints=True, retries=3)
def transform_data(df):
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    print(f"pre: missing passenger count: {(df['passenger_count'] == 0).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {(df['passenger_count'] == 0).sum()}")
    return df

@task(log_prints=True, retries=3)
def ingest_data(df, table_name, engine):
    df.head(n=0).to_sql(table_name, engine, if_exists='replace', index=False)
    df.to_sql(table_name, engine, if_exists='append', index=False)

@flow(name="Subflow", log_prints=True)
def log_subflow(table_name):
    print("Logging Subflow for : {table_name}")

@flow(name='Ingest Data')
def main_flow():
    user = 'root'
    password = 'root'
    host = 'localhost'
    port = '5432'
    db = 'ny_taxi'
    table_name = 'yellow_taxi_trips'
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-03.parquet'
    log_subflow(table_name)
    raw_data = extract_data(url)
    transformed_data = transform_data(raw_data)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    ingest_data(transformed_data, table_name, engine)
    

if __name__ == '__main__':
    main_flow()