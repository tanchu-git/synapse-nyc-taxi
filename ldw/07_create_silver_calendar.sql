USE nyc_taxi_ldw;
-----------------------------------------------------------
-- create silver table for calendar
-----------------------------------------------------------
IF OBJECT_ID('silver.calendar') IS NOT NULL
    DROP EXTERNAL TABLE silver.calendar
GO

CREATE EXTERNAL TABLE silver.calendar
    WITH (
        LOCATION = 'silver/calendar',
        DATA_SOURCE = nyc_taxi_ext_source,
        FILE_FORMAT = parquet_file_format
    )
AS
SELECT *
    FROM bronze.calendar;

SELECT * FROM silver.calendar;