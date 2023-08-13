USE nyc_taxi_ldw
GO
-----------------------------------------------------------
-- put the CETAS into a stored procedure
-----------------------------------------------------------
CREATE OR ALTER PROCEDURE silver.sp_silver_vendor
AS
BEGIN
    IF OBJECT_ID('silver.vendor') IS NOT NULL
        DROP EXTERNAL TABLE silver.vendor

    CREATE EXTERNAL TABLE silver.vendor
        WITH (
            LOCATION = 'silver/vendor',
            DATA_SOURCE = nyc_taxi_ext_source,
            FILE_FORMAT = parquet_file_format
        )
    AS
    SELECT *
        FROM bronze.vendor;
END;