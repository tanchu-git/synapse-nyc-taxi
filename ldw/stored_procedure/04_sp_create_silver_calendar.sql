USE nyc_taxi_ldw
GO
-----------------------------------------------------------
-- put the CETAS into a stored procedure
-----------------------------------------------------------
CREATE OR ALTER PROCEDURE silver.sp_silver_calendar
AS
BEGIN
    IF OBJECT_ID('silver.calendar') IS NOT NULL
        DROP EXTERNAL TABLE silver.calendar
    
    CREATE EXTERNAL TABLE silver.calendar
        WITH (
            LOCATION = 'silver/calendar',
            DATA_SOURCE = nyc_taxi_ext_source,
            FILE_FORMAT = parquet_file_format
        )
    AS
    SELECT *
        FROM bronze.calendar;
END;