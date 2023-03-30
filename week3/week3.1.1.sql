-- Creates an external table on top of the data in GCS
CREATE OR REPLACE EXTERNAL TABLE `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.external_yellow_tripdata`
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://week2-prefect-de-zoomcamp/data/yellow_tripdata/yellow_tripdata_2021-*.parquet',
            'gs://week2-prefect-de-zoomcamp/data/yellow_tripdata/yellow_tripdata_2022-*.parquet']
);

-- Check the external table
SELECT * FROM `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.external_yellow_tripdata` LIMIT 10;

-- Create a non-partitioned table from the external table
CREATE OR REPLACE TABLE `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.yellow_tripdata_non_partitioned` AS SELECT * FROM `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.external_yellow_tripdata`;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.yellow_tripdata_partitioned` PARTITION BY DATE(tpep_pickup_datetime) AS SELECT * FROM `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.external_yellow_tripdata`;

-- Compare the performance when querying the non-partitioned table and the partitioned table
-- This query will process 1.05 GB when run.
SELECT DISTINCT(PULocationID) FROM `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.yellow_tripdata_non_partitioned` WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' AND '2021-06-30';

-- This query will process 185.84 MB when run.
SELECT DISTINCT(PULocationID) FROM `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.yellow_tripdata_partitioned` WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' AND '2021-06-30';

-- Look into the partitions
SELECT table_name, partition_id, total_rows FROM `week2_prefect_de_zoomcamp.INFORMATION_SCHEMA.PARTITIONS` WHERE table_name = 'yellow_tripdata_partitioned' ORDER BY total_rows DESC;

-- Create a clustered data
CREATE OR REPLACE TABLE `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.yellow_tripdata_partitioned_clustered` 
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY PULocationID AS
SELECT * FROM `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.external_yellow_tripdata`;

-- Compare the performance when querying unclustered and clustered data
-- Bytes processed 1.05 GB
-- Bytes billed 1.05 GB
SELECT COUNT(*) AS trips
FROM `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' and '2022-12-31'
AND PULocationID = 132;

-- This query will process 369.52 MB when run.
-- Bytes processed 992.58 MB
-- Bytes billed 993 MB
SELECT COUNT(*) AS trips
FROM `yellow-taxi-trips-data-382114.week2_prefect_de_zoomcamp.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2021-01-01' and '2022-12-31'
AND PULocationID = 132;

