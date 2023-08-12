# Azure Synapse Analytics NYC Taxi Project
Data discovery, ingestion and transformation of the dataset will be done with Azure Synapse Analytics platform.

![Screenshot 2023-08-12 003049](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/b22aa5d5-eaf2-4a11-8278-826a18e8b76c)

## TLC Trip Record Data
The [datasets](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) were collected and provided by technology providers, authorized by the NYC Taxi and Limousine Commission (TLC) . Yellow and green taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts.

#### Data Model
![260258628-c96324e3-f449-49e5-b22c-e500deeb7572](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/55d92ac5-c0c7-458a-b073-4fa59e9c0ff5)

## Synapse Workspace and Raw Dataset
When creating a new workspace, a new or existing Data Lake Storage Gen2 account needs to be attached to it. Credentials for access to the workspace's SQL pool will also be generated at creation. I will keep the dataset in ```nyc-taxi-data``` container, created in the workspace attached storage account.

We can access the container in Synapse Studio. Raw dataset will be uploaded to the ```raw``` folder.

![Screenshot 2023-08-12 231137](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/ed959054-3b84-448c-8517-fe5d1d878278)

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



