
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://udemysynapsecourse.dfs.core.windows.net/nyc-taxi-data/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]

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

-----------------------------------------------------------
-- Examine data types for the columns
-----------------------------------------------------------
EXEC sp_describe_first_result_set N'SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ''abfss://nyc-taxi-data@udemysynapsecourse.dfs.core.windows.net/raw/taxi_zone.csv'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        HEADER_ROW = TRUE
    ) AS [result]'

-----------------------------------------------------------
-- Check max len of columns
-----------------------------------------------------------
SELECT
    MAX(LEN(LocationID)) AS len_locationId,
    MAX(LEN(Borough)) AS len_borough,
    MAX(LEN(Zone)) AS len_zone,
    MAX(LEN(service_zone)) AS len_service_zone
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@udemysynapsecourse.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) AS [result]

-----------------------------------------------------------
-- Use WITH clause to define explicit data types
-----------------------------------------------------------
SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@udemysynapsecourse.dfs.core.windows.net/raw/taxi_zone.csv',
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

-- See the changes of data types
EXEC sp_describe_first_result_set N'SELECT
    *
FROM
    OPENROWSET(
        BULK ''abfss://nyc-taxi-data@udemysynapsecourse.dfs.core.windows.net/raw/taxi_zone.csv'',
        FORMAT = ''CSV'',
        PARSER_VERSION = ''2.0'',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = '','',
        ROWTERMINATOR = ''\n''
    ) 
    WITH (
        LocationID SMALLINT,
        Borough VARCHAR(15),
        Zone VARCHAR(50),
        service_zone VARCHAR(15)
    )
    AS [result]'

-----------------------------------------------------------
-- Check collation
-----------------------------------------------------------
SELECT name, collation_name FROM sys.databases;
-----------------------------------------------------------
-- Change collation in WITH clause
-----------------------------------------------------------
SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@udemysynapsecourse.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        LocationID SMALLINT,
        Borough VARCHAR(15) COLLATE Latin1_General_100_CI_AI_SC_UTF8,
        Zone VARCHAR(50) COLLATE Latin1_General_100_CI_AI_SC_UTF8,
        service_zone VARCHAR (15) COLLATE Latin1_General_100_CI_AI_SC_UTF8
    )
    AS [result]
-----------------------------------------------------------
-- Create new database to use
-----------------------------------------------------------
CREATE DATABASE nyc_taxi_discovery;

-- Change collation at the database level
USE nyc_taxi_discovery;

ALTER DATABASE nyc_taxi_discovery COLLATE Latin1_General_100_CI_AI_SC_UTF8;

-- Check collation
SELECT name, collation_name FROM sys.databases;

-----------------------------------------------------------
-- Select columns with ordinal position on csv file without header
-----------------------------------------------------------
SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@udemysynapsecourse.dfs.core.windows.net/raw/taxi_zone_without_header.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        Zone VARCHAR(50) 3,
        Borough VARCHAR(15) 2
    )
AS [result]

-----------------------------------------------------------
-- Fix column names with ordinal position
-----------------------------------------------------------
SELECT
    *
FROM
    OPENROWSET(
        BULK 'abfss://nyc-taxi-data@udemysynapsecourse.dfs.core.windows.net/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIRSTROW = 2, -- Specify starting row
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        location_id SMALLINT 1,
        borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone VARCHAR(15) 4
    )
 AS [result]

-----------------------------------------------------------
 -- Create External Data Source
 -----------------------------------------------------------
 CREATE EXTERNAL DATA SOURCE nyc_taxi_data
 WITH(
    LOCATION = 'abfss://nyc-taxi-data@udemysynapsecourse.dfs.core.windows.net/'
 )

CREATE EXTERNAL DATA SOURCE nyc_taxi_data_raw
 WITH(
    LOCATION = 'abfss://nyc-taxi-data@udemysynapsecourse.dfs.core.windows.net/raw/'
 )
-----------------------------------------------------------
-- Use external data source to point to csv file
-----------------------------------------------------------
SELECT
    *
FROM
    OPENROWSET(
        BULK 'taxi_zone.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIRSTROW = 2, -- Specify starting row
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        location_id SMALLINT 1,
        borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone VARCHAR(15) 4
    )
AS [result]

DROP EXTERNAL DATA SOURCE nyc_taxi_data

SELECT name, location FROM sys.external_data_sources;