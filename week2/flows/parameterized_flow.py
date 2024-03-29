from pathlib import Path 
import pandas as pd 
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(log_prints=True, retries=5, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract(dataset_url: str) -> pd.DataFrame:
    """
    Extracts the dataset from the URL
    """
    df = pd.read_parquet(dataset_url)
    return df

@task(log_prints=True)
def transform(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """
    Transforms the dataset
    """
    if color == "yellow":
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    elif color == "green":
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """
    Writes the transformed dataset to a local file
    """
    path = Path(f"data/{color}_tripdata/{dataset_file}.parquet")
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)
    return path

@task(log_prints=True)
def write_gcs(file_path: Path) -> None:
    """
    Writes the transformed dataset to GCS
    """
    gcp_cloud_storage_bucket_block = GcsBucket.load("week2-web-to-gcs-connector")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{file_path}",
        to_path=file_path
    )
    return

@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """
    The main ETL function
    """
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"

    df = extract(dataset_url)
    df_transformed = transform(df, color)
    path = write_local(df_transformed, color, dataset_file)
    print(path)
    write_gcs(path)

@flow()
def etl_parent_flow(months: list[int], year: int, color: str = "yellow"):
    for month in months:
        etl_web_to_gcs(year=year, month=month, color=color)

if __name__ == "__main__":
    color = "yellow"
    year = 2021
    months = [1, 2, 3]
    etl_parent_flow(months, year, color)