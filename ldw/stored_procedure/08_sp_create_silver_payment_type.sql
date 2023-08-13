USE nyc_taxi_ldw
GO
-----------------------------------------------------------
-- put the VIEW into a stored procedure
-----------------------------------------------------------
CREATE OR ALTER PROCEDURE silver.sp_silver_payment_type
AS
BEGIN
    IF OBJECT_ID('silver.payment_type') IS NOT NULL
        DROP EXTERNAL TABLE silver.payment_type

    CREATE EXTERNAL TABLE silver.payment_type
        WITH (
            LOCATION = 'silver/payment_type',
            DATA_SOURCE = nyc_taxi_ext_source,
            FILE_FORMAT = parquet_file_format
        )
    AS
    SELECT * FROM bronze.view_payment_type;
END;