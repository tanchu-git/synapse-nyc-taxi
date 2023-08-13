USE nyc_taxi_ldw;
-----------------------------------------------------------
-- create silver table for vendor
-----------------------------------------------------------
IF OBJECT_ID('silver.vendor') IS NOT NULL
    DROP EXTERNAL TABLE silver.vendor
GO

CREATE EXTERNAL TABLE silver.vendor
    WITH (
        LOCATION = 'silver/vendor',
        DATA_SOURCE = nyc_taxi_ext_source,
        FILE_FORMAT = parquet_file_format
    )
AS
SELECT *
    FROM bronze.vendor;

SELECT * FROM silver.vendor;