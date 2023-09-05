# Azure Synapse Analytics NYC Taxi Project
Data discovery, ingestion and transformation of the dataset will be done with Azure Synapse Analytics platform.

## Solution Architecture
![Screenshot 2023-08-12 003049](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/b22aa5d5-eaf2-4a11-8278-826a18e8b76c)

## TLC Trip Record Data
The [datasets](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) were collected and provided by technology providers, authorized by the NYC Taxi and Limousine Commission (TLC) . They feature fields like *pick-up and drop-off locations*, *trip distances*, *rate types*, *payment types* and many more.

#### Data Model
![260258628-c96324e3-f449-49e5-b22c-e500deeb7572](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/55d92ac5-c0c7-458a-b073-4fa59e9c0ff5)

## Synapse Workspace and Raw Dataset
When creating a new workspace, a new or existing Data Lake Storage Gen2 account needs to be attached to it. I created the ```nyc-taxi-data``` container in the attached storage account, to store the dataset. Credentials for access to the workspace's SQL pool will also be generated at creation. 

With access to the container in Synapse Studio, raw dataset will be uploaded to the ```raw``` folder.

![260260798-ed959054-3b84-448c-8517-fe5d1d878278](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/16abe617-2ecf-4e45-87f2-ebfc3793e104)

#### Before I can extract insights from the dataset, basic discovery and exploration of the data in various formats (Parquet, CSV, JSON) will be done with Serverless SQL pool.

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

Data types are the usual suspects, in terms of increasing performance.
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
Serverless SQL pool has no local storage, only metadata objects are stored in database. The raw dataset is already present in my blob container, ```CREATE EXTERNAL TABLE``` clause will be used to create the *bronze* tables. 

First I create my logical data warehouse as database ```nyc_taxi_ldw``` with three schemas - ```bronze```, ```silver``` and ```gold```. Next is my external data source object ```nyc_taxi_ext_source``` and external file format objects.

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
Now that the data is prepared, transformations can begin to extract business value - like payment behaviour. Do people prefer card or cash payment? Does the preference change over the weekend and between boroughs? Let's find out with some ```JOIN``` clauses and aggregate functions.
```sql
USE nyc_taxi_ldw;

SELECT trip_data.year, 
       trip_data.month, 
       taxi_zone.borough,
       CONVERT(DATE, trip_data.lpep_pickup_datetime) AS trip_date,
       calendar.day_name AS trip_day,       
       CASE WHEN calendar.day_name IN ('Saturday', 'Sunday') THEN 'Y' ELSE 'N' END AS is_weekend,
       SUM(CASE WHEN payment_type.description = 'Credit card' THEN 1 ELSE 0 END) AS card_trip_count,
       SUM(CASE WHEN payment_type.description = 'Cash' THEN 1 ELSE 0 END) AS cash_trip_count       
    FROM silver.view_trip_data_green trip_data
    JOIN silver.taxi_zone 
        ON trip_data.pu_location_id = taxi_zone.location_id
    JOIN silver.calendar
        ON calendar.date = CONVERT(DATE, trip_data.lpep_pickup_datetime)
    JOIN silver.payment_type
        ON trip_data.payment_type = payment_type.payment_type
GROUP BY trip_data.year, 
         trip_data.month, 
         taxi_zone.borough,
         CONVERT(DATE, trip_data.lpep_pickup_datetime),
         calendar.day_name
```
Gold table query result.

![Screenshot 2023-08-14 231229](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/be24b590-9f90-47a0-8064-879f0ec1b6d5)

To improve the performance of the query, I created a [stored procedure](https://github.com/tanchu-git/synapse_nyc_taxi/tree/main/ldw#there-are-some-tables-created-with-partitions-in-mind-using-stored-procedure) to partition the data by year and month.
## Automated pipeline
Data consumers will want the data up to date. For that I created pipelines that runs at an regular interval. It starts with a script to look up the bronze tables, deletes old silver partitions and finally create new ones with the stored query. Same procedure, going from silver to gold.

![Screenshot 2023-08-15 002226](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/c2ea85e9-1dde-4665-8244-ad2bc7b376ee)

Some silver tables require a different pipeline design, so there will be two pipeline designs for silver layer. Orchestrating these pipelines together. Gold layer will stay up to date, as new data gets added through the bronze layer.

![Screenshot 2023-08-15 012506](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/fa4b5c3e-82cc-4c88-ac49-1c6ccad4c313)

## Power BI
Synapse Studio offers a seamless way to connect a Power BI workspace to an Azure Synapse Analytics workspace, which makes it possible to create new Power BI reports from Synapse Studio.

![Screenshot 2023-08-15 220652](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/d52de5c6-bda2-4152-8705-acaedc430b7c)

![image](https://github.com/tanchu-git/synapse_nyc_taxi/assets/139019601/d638131e-82c5-4b69-8730-9e97b6f832d2)

Covid hit really hard during the first quarter of 2020.

## Dedicated SQL Pool
Dedicated SQL pool stores data internally, in relational tables with columnar storage. Once data is stored, you can run analytics at massive scale with its distributed query engine using T-SQL. I have only been using Serverless SQL Pool to work with, since it is better suited for small datasets. 

If the dataset ever grows to a size where a Dedicated SQL Pool would make sense, [here](https://github.com/tanchu-git/synapse_nyc_taxi/tree/main/dwh) are some methods to copy data from external storage into Dedicated SQL Pool.
