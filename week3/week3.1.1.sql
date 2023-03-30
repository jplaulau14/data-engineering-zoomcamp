CREATE OR REPLACE EXTERNAL TABLE `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.external_yellow_tripdata`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://week2-prefect-de-zoomcamp/data/yellow_tripdata/yellow_tripdata_2021-*.parquet']
);