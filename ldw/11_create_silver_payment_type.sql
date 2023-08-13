USE nyc_taxi_ldw;
-----------------------------------------------------------
-- create silver table for JSON payment_type
-- can use VIEW - SELECT * FROM bronze.view_payment_type
-- can use SELECT * FROM OPENROWSET ***
-----------------------------------------------------------
IF OBJECT_ID('silver.payment_type') IS NOT NULL
    DROP EXTERNAL TABLE silver.payment_type
GO

CREATE EXTERNAL TABLE silver.payment_type
    WITH (
        LOCATION = 'silver/payment_type',
        DATA_SOURCE = nyc_taxi_ext_source,
        FILE_FORMAT = parquet_file_format
    )
AS
SELECT * FROM bronze.view_payment_type;

SELECT * FROM silver.payment_type;