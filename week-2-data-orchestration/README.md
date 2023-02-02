# Data Engineering Zoomcamp - Week 2

## Data Lake
### What is a Data Lake?
- A Data Lake consists of a central repository where any type of data, either structured or unstructured, can be stored. The main idea behind a Data Lake is to ingest and make data available as quickly as possible inside an organization.

### Data Lake vs Data Warehouse
- A Data Lake stores a huge amount of data and are normally used for stream processing, machine learning, and real time analytics. On the other hand, a Data Warehouse stores structured data for analytics and batch processing.

### Extract-Tranform-Load (ETL) vs. Extract-Load-Transform (ELT)
- ETL is usually a Data Warehouse solution, used mainly for small amount of data as a schema-on-write approach. On the other hand, ELT is a Data Lake solution, employed for large amounts of data as schema-on-read approach.

## Introduction to Workflow Orchestration
### What is dataflow?
- A dataflow defines all extraction and processing steps that the data will be submitted to, also detailing any transformation and intermediate states of the dataset. For example, in an ETL process a dataset is first extracted (E) from source (e.g., website, API, etc), then transformed (T) (e.g., dealing with corrupted or missing values, joining datasets, datatype conversion, etc) and finally loaded (L) to some type of storage (e.g., data warehouse).

### What is workflow orchestration?
- A workflow orchestration tool allows us to manage and visualize dataflows, while ensuring that they will be run according to a set of predefined rules. A good workflow orchestration tool makes it easy to schedule or execute dataflows remotely, handle faults, integrate with external services, increase reliability, etc.

### Introduction to Prefect concepts
