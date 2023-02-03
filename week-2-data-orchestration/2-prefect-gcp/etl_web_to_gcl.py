from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials



@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """
    Fetch data from web into a pandas dataframe
    """
    df = pd.read_parquet(dataset_url, engine='pyarrow')
    return df

@task(log_prints=True, retries=3)
def clean(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Clean the data
    """
    print(f"pre: column dtypes: {df.dtypes}")
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(f"post: column dtypes: {df.dtypes}")
    
    print(f"pre: missing passenger count: {(df['passenger_count'] == 0).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {(df['passenger_count'] == 0).sum()}")

    print(f"pre: data with year before {year}: {df.tpep_pickup_datetime.dt.year.unique()}")
    df = df[df.tpep_pickup_datetime.dt.year == year]
    print(f"post: data with year before {year}: {df.tpep_pickup_datetime.dt.year.unique()}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> str:
    """
    Write the data to a local file
    """
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip", index=False)
    return path

@task()
def write_gcs(path: Path) -> None:
    """
    Write the data to a GCS bucket
    """
    gcp_cloud_storage_bucket_block = GcsBucket.load("gcs-bucket-block")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=path,
    )

@flow()
def etl_web_to_gcs() -> None:
    """
    Main ETL function
    """
    color = "yellow"
    year = 2022
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"

    df = fetch(dataset_url)
    df_clean = clean(df, year)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()