-- Creates an external table on top of the data in GCS
CREATE OR REPLACE EXTERNAL TABLE `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.external_yellow_tripdata`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://week2-prefect-de-zoomcamp/data/yellow_tripdata/yellow_tripdata_2021-*.parquet']
);

-- Check the external table
SELECT * FROM `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.external_yellow_tripdata` LIMIT 10;

-- Create a non-partitioned table from the external table
CREATE OR REPLACE TABLE `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.yellow_tripdata_non_partitioned` AS SELECT * FROM `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.external_yellow_tripdata`;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.yellow_tripdata_partitioned` PARTITION BY DATE(tpep_pickup_datetime) AS SELECT * FROM `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.external_yellow_tripdata`;