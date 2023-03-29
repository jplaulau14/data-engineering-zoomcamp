from pathlib import Path 
import pandas as pd 
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True, retries=5)
def extract(dataset_url: str) -> pd.DataFrame:
    """
    Extracts the dataset from the URL
    """
    df = pd.read_parquet(dataset_url)
    return df

@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the dataset
    """
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
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
def etl_web_to_gcs() -> None:
    """
    The main ETL function
    """
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"

    df = extract(dataset_url)
    df_transformed = transform(df)
    path = write_local(df_transformed, color, dataset_file)
    print(path)
    write_gcs(path)

if __name__ == "__main__":
    etl_web_to_gcs()