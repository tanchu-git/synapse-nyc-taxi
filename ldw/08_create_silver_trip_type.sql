USE nyc_taxi_ldw;
-----------------------------------------------------------
-- create silver table for trip_type
-----------------------------------------------------------
IF OBJECT_ID('silver.trip_type') IS NOT NULL
    DROP EXTERNAL TABLE silver.trip_type
GO

CREATE EXTERNAL TABLE silver.trip_type
    WITH (
        LOCATION = 'silver/trip_type',
        DATA_SOURCE = nyc_taxi_ext_source,
        FILE_FORMAT = parquet_file_format
    )
AS
SELECT *
    FROM bronze.trip_type;

SELECT * FROM silver.trip_type;