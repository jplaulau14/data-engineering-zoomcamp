from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True, retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """
    Download trip data from GCS
    """
    gcs_path = f"gcs/{color}/{color}_tripdata_{year}-{month:02d}.parquet"
    gcs_block = GcsBucket.load("gcs-bucket-block")
    gcs_block.get_directory(
        from_path=gcs_path,
        local_path=f"data/"
    )
    return Path(f"data/{gcs_path}")

@task(log_prints=True, retries=3)
def transform(path: Path, year: int) -> pd.DataFrame:
    """
    Data cleaning
    """
    df = pd.read_parquet(path, engine='pyarrow')
    print(f"pre: missing passenger count: {(df['passenger_count'] == 0).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {(df['passenger_count'] == 0).sum()}")

    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df.passenger_count = df.passenger_count.fillna(0)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")

    print(f"pre: data with year before {year}: {df.tpep_pickup_datetime.dt.year.unique()}")
    df = df[df.tpep_pickup_datetime.dt.year == year]
    print(f"post: data with year before {year}: {df.tpep_pickup_datetime.dt.year.unique()}")
    return df

@task(log_prints=True, retries=3)
def write_bq(df: pd.DataFrame) -> None:
    """
    Write the data to a Big Query table
    """
    gcp_creds = GcpCredentials.load("prefect-gcs")

    df.to_gbq(
        destination_table="2_prefect_dataset.ny_trips",
        project_id="dtc-de-zoomcamp-376217",
        credentials=gcp_creds.get_credentials_from_service_account(),
        chunksize=100000,
        if_exists="append",
    )

@flow()
def etl_gcs_to_bq() -> None:
    """
    Main ETL function to load data into Big Query
    """
    color = "yellow"
    year = 2022
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform(path, year)
    write_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq()