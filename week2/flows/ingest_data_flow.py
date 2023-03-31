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
    df = pd.read_parquet(url)
    return df

@task(log_prints=True)
def transform_data(df, color):
    if color == "yellow":
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    elif color == "green":
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    return df

@task(log_prints=True)
def load_data(df, table_name):
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.to_sql(name=table_name, con=engine, if_exists='append')
    
@flow(name="Ingest Flow")
def etl_web_to_postgres(year: int, month: int, color: str):
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"
    table_name = f"{color}_taxi_trips"
    df = extract_data(dataset_url)
    df_transformed = transform_data(df, color)
    load_data(df_transformed, table_name)

@flow()
def etl_parent_flow(months: list[int], years: list[int], color: str = "yellow"):
    for year in years:
        for month in months:
            etl_web_to_postgres(year=year, month=month, color=color)

if __name__ == '__main__':
    months = [1, 2, 3]
    years = [2021, 2022]
    color = "yellow"
    etl_parent_flow(months, years, color)