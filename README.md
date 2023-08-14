# Azure Synapse Analytics NYC Taxi Project
Data discovery, ingestion and transformation of the dataset will be done with Azure Synapse Analytics platform.

![Screenshot 2023-08-12 003049](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/b22aa5d5-eaf2-4a11-8278-826a18e8b76c)

## TLC Trip Record Data
The [datasets](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) were collected and provided by technology providers, authorized by the NYC Taxi and Limousine Commission (TLC) . Yellow and green taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.

#### Data Model
![260258628-c96324e3-f449-49e5-b22c-e500deeb7572](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/55d92ac5-c0c7-458a-b073-4fa59e9c0ff5)

## Synapse Workspace and Raw Dataset
When creating a new workspace, a new or existing Data Lake Storage Gen2 account needs to be attached to it. I created the ```nyc-taxi-data``` container in the attached storage account, to store the dataset. Credentials for access to the workspace's SQL pool will also be generated at creation. 

Basic discovery and exploration of the data in various formats (Parquet, CSV, JSON) will be done with Synapse Serverless SQL pool, so I can plan how to extract insights from it. With access to the container in Synapse Studio, raw dataset will be uploaded to the ```raw``` folder.

![260260798-ed959054-3b84-448c-8517-fe5d1d878278](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/16abe617-2ecf-4e45-87f2-ebfc3793e104)

## Data Exploration
Checking for headers and other characteristics of the dataset.
```sql
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@udemysynapsecourse.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) AS [result]
```
![Screenshot 2023-08-12 235247](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/43af9688-51de-41de-b9f0-5c746622755c)

Data types will be the usual suspects, in terms of increasing performance.
```sql
EXEC sp_describe_first_result_set N'SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ''abfss://nyc-taxi-data@udemysynapsecourse.dfs.core.windows.net/raw/taxi_zone.csv'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        HEADER_ROW = TRUE
    ) AS [result]'
```
As you can see, Synapse is being a bit generous here with allocating the data size of the columns.

![Screenshot 2023-08-12 235838](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/9e39b36f-a654-439b-aa44-4da000b47927)

Using ```Max( LEN(<column>) )``` to find appropriate data size for the columns. An external data source object (```nyc_taxi_data```) that points to my container, is created. And now the ```WITH``` clause to define explicit data types.
```sql
SELECT
    *
FROM
    OPENROWSET(
        BULK 'raw/taxi_zone.csv',
        DATA_SOURCE = 'nyc_taxi_data',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        LocationID SMALLINT,
        Borough VARCHAR(15),
        Zone VARCHAR(50),
        service_zone VARCHAR(15)
    )
    AS [result]
```
### The dataset comes in varous file formats like ```JSON```, ```PARQUET``` and ```DELTA```. More exploration can be found in the [discovery folder](https://github.com/tanchu-git/synapse_nyc_taxi/tree/main/discovery).

## Data Discovery
Checking for null records.
```sql
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS trip_data_green_parquet
WHERE PULocationID is NULL; -- query returns nothing if there are no nulls
```
The ```trip_data``` table have got the ```PULocationID``` column and not the ```borough``` column. While ```taxi_zone``` have got both columns. Joining these two tables, to identify number of trips made from each borough.
```sql
SELECT 
    taxi_zone.borough,
    COUNT(*) AS number_of_trips
FROM 
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS trip_data
JOIN 
    OPENROWSET( -- OPENROWSET retuns a table, join on this
    BULK 'taxi_zone.csv',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    FIRSTROW = 2 -- Specify starting row
    ) 
    WITH (
        location_id SMALLINT 1, -- specify column with ordinal position
        borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone VARCHAR(15) 4
    ) AS taxi_zone
ON trip_data.PULocationID = taxi_zone.location_id
GROUP BY taxi_zone.borough
ORDER BY number_of_trips DESC;
```
![Screenshot 2023-08-13 225327](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/18c5a5c0-9e02-4983-9889-f5dc73a7876a) ![Screenshot 2023-08-13 225550](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/cd5763cd-ded8-41f7-bda8-6f9251519dbd)

### More discovery can be found in the [discovery folder](https://github.com/tanchu-git/synapse_nyc_taxi/tree/main/discovery).

## Data Virtualization
Serverless SQL pool has no local storage, only metadata objects are stored in databases. A logical data warehouse, allows me to combine data from multiple sources and save query results for downstream use without having to load data from ETL pipelines. The raw dataset is already present in my blob container, ```CREATE EXTERNAL TABLE``` clause will be used to create the *bronze* tables. 

First I create my logical data warehouse as database ```nyc_taxi_ldw``` with three schemas - ```bronze```, ```silver``` and ```gold```. Next is my external data source object ```nyc_taxi_ext_source``` and external file format objects  ```csv_file_format```, ```tsv_file_format```, ```parquet_file_format``` and ```delta_file_format```.

Now the bronze table. External tables points to data located in a data lake.
```sql
USE nyc_taxi_ldw;

IF OBJECT_ID('bronze.taxi_zone') IS NOT NULL
    DROP EXTERNAL TABLE bronze.taxi_zone;

CREATE EXTERNAL TABLE bronze.taxi_zone
    (
        location_id SMALLINT,
        borough VARCHAR(15),
        zone VARCHAR(50),
        service_zone VARCHAR(15)
    )
    WITH (
        LOCATION = 'raw/taxi_zone.csv',
        DATA_SOURCE = nyc_taxi_ext_source, -- points to my blob containing the raw data
        FILE_FORMAT = csv_file_format, -- defines the file format
        REJECT_VALUE = 10,
        REJECTED_ROW_LOCATION = 'rejections/taxi_zone'
    );
```
## Data Ingestion
When using serverless SQL pool, CETAS is used to create an external table and *export* query results to an external storage. So *silver* and *gold* tables will be using ```CREATE EXTERNAL TABLE AS SELECT``` clause.

Using bronze table to create table in silver schema, and ingest data as parquet format into the silver folder in my blob container.
```sql
USE nyc_taxi_ldw;

IF OBJECT_ID('silver.taxi_zone') IS NOT NULL
    DROP EXTERNAL TABLE silver.taxi_zone
GO

CREATE EXTERNAL TABLE silver.taxi_zone
    WITH (
        LOCATION = 'silver/taxi_zone',
        DATA_SOURCE = nyc_taxi_ext_source,
        FILE_FORMAT = parquet_file_format -- exporting data as parquet
    )
AS
SELECT *
    FROM bronze.taxi_zone;
```
### More tables creation and ingestion can be found in the [ldw folder](https://github.com/tanchu-git/synapse_nyc_taxi/tree/main/ldw)

## Data Transformation
