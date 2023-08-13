USE nyc_taxi_ldw
GO
-----------------------------------------------------------
-- put the CETAS into a stored procedure
-----------------------------------------------------------
CREATE OR ALTER PROCEDURE silver.sp_silver_trip_type
AS
BEGIN
    IF OBJECT_ID('silver.trip_type') IS NOT NULL
        DROP EXTERNAL TABLE silver.trip_type

    CREATE EXTERNAL TABLE silver.trip_type
        WITH (
            LOCATION = 'silver/trip_type',
            DATA_SOURCE = nyc_taxi_ext_source,
            FILE_FORMAT = parquet_file_format
        )
    AS
    SELECT *
        FROM bronze.trip_type;
END;