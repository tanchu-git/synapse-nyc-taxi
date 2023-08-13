USE nyc_taxi_ldw
GO
-----------------------------------------------------------
-- create silver table for taxi_zone
-- USE CETAS (CREATE EXTERNAL TABLE AS SELECT)
-- using bronze file to transform into parquet file

-- put the CETAS into a stored procedure
-----------------------------------------------------------
CREATE OR ALTER PROCEDURE silver.sp_silver_taxi_zone
AS
BEGIN
    IF OBJECT_ID('silver.taxi_zone') IS NOT NULL
        DROP EXTERNAL TABLE silver.taxi_zone;

    CREATE EXTERNAL TABLE silver.taxi_zone
        WITH (
            LOCATION = 'silver/taxi_zone',
            DATA_SOURCE = nyc_taxi_ext_source,
            FILE_FORMAT = parquet_file_format
        )
    AS
    SELECT *
        FROM bronze.taxi_zone;
END;