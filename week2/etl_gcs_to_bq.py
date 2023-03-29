from pathlib import Path 
import pandas as pd 
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True, retries=5)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """
    Extracts the dataset from GCS
    """
    gcs_path = f"data/{color}_tripdata/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("week2-web-to-gcs-connector")
    gcs_block.download_object_to_path(
        from_path=gcs_path,
        to_path=Path(f"data/{color}_tripdata/{color}_tripdata_{year}-{month:02}.parquet")
    )
    return Path(f"data/{color}_tripdata/{color}_tripdata_{year}-{month:02}.parquet")

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """
    Transforms the dataset
    """
    df = pd.read_parquet(path)
    print(f"Pre: Missing Passenger Count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'] = df['passenger_count'].fillna(0)
    print(f"Post: Missing Passenger Count: {df['passenger_count'].isna().sum()}")
    return df

@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """
    Writes the transformed dataset to BQ
    """
    gcp_credentials_block = GcpCredentials.load("week2-gcp-creds")
    df.to_gbq(
        destination_table="week2_prefect_de_zoomcamp.rides",
        project_id="yellow-taxi-trips-data-382114",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )
@flow()
def etl_gcs_to_bq():
    """
    The main ETL function
    """
    color = "yellow"
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq()