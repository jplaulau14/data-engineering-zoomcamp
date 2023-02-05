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
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> str:
    """
    Write the data to a local file
    """
    if not Path(f"data/gcs/{color}").exists():
        Path(f"data/gcs/{color}").mkdir(parents=True)

    path = Path(f"data/gcs/{color}/{dataset_file}.parquet")
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

@flow(log_prints=True, retries=3)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """
    Main ETL function
    """
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}.parquet"

    df = fetch(dataset_url)
    df_clean = clean(df, year)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(
    months: list[int] = [1, 2, 3], year: int = 2021, color: str = "yellow"
) -> None:
    for month in months:
        etl_web_to_gcs(year, month, color)

if __name__ == '__main__':
    color="yellow"
    year=2021
    months=[1, 2, 3]
    etl_parent_flow(months, year, color)