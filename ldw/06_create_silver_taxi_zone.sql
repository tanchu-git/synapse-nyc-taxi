USE nyc_taxi_ldw;
-----------------------------------------------------------
-- create silver table for taxi_zone
-- USE CETAS (CREATE EXTERNAL TABLE AS SELECT)
-- using bronze file to transform into parquet file
-----------------------------------------------------------
IF OBJECT_ID('silver.taxi_zone') IS NOT NULL
    DROP EXTERNAL TABLE silver.taxi_zone
GO

CREATE EXTERNAL TABLE silver.taxi_zone
    WITH (
        LOCATION = 'silver/taxi_zone',
        DATA_SOURCE = nyc_taxi_ext_source,
        FILE_FORMAT = parquet_file_format
    )
AS
SELECT *
    FROM bronze.taxi_zone;

SELECT * FROM silver.taxi_zone;