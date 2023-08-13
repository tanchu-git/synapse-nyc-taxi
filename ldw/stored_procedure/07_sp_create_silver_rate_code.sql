USE nyc_taxi_ldw
GO
-----------------------------------------------------------
-- put the CETAS into a stored procedure
-----------------------------------------------------------
CREATE OR ALTER PROCEDURE silver.sp_silver_rate_code
AS
BEGIN
    IF OBJECT_ID('silver.rate_code') IS NOT NULL
        DROP EXTERNAL TABLE silver.rate_code

    CREATE EXTERNAL TABLE silver.rate_code
        WITH (
            LOCATION = 'silver/rate_code',
            DATA_SOURCE = nyc_taxi_ext_source,
            FILE_FORMAT = parquet_file_format
        )
    AS
    SELECT rate_code_id, rate_code
        FROM OPENROWSET(
            BULK 'raw/rate_code.json',
            DATA_SOURCE = 'nyc_taxi_ext_source',
            FORMAT = 'csv',
            FIELDTERMINATOR = '0x0b',
            FIELDQUOTE = '0x0b',
            ROWTERMINATOR = '0x0b' -- vertical tab
        )
        WITH(
            jsonDoc NVARCHAR(MAX)
        )
        AS payment_type
        CROSS APPLY OPENJSON(jsonDoc)
        WITH(
            rate_code_id TINYINT,
            rate_code VARCHAR(25) -- '$.rate_code' -- renamed
        )
END;