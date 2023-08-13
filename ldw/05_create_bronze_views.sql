USE nyc_taxi_ldw
GO
-----------------------------------------------------------
-- Create view for rate_code
-----------------------------------------------------------
DROP VIEW IF EXISTS bronze.view_rate_code
GO

CREATE VIEW bronze.view_rate_code
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
GO

SELECT * FROM bronze.view_rate_code
GO

-----------------------------------------------------------
-- Create view for payment_type
-----------------------------------------------------------
DROP VIEW IF EXISTS bronze.view_payment_type
GO

CREATE VIEW bronze.view_payment_type
AS
SELECT payment_type, description
    FROM OPENROWSET(
        BULK 'raw/payment_type.json',
        DATA_SOURCE = 'nyc_taxi_ext_source',
        FORMAT = 'csv',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0a' --\n - default
    )
    WITH(
        jsonDoc NVARCHAR(MAX)
    )
    AS payment_type
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        payment_type SMALLINT,
        description VARCHAR(20) '$.payment_type_desc' -- renaming
    )AS final_payment_type;
GO

SELECT * FROM bronze.view_payment_type
GO

-----------------------------------------------------------
-- Use view to prune by partition. 
-- External tables can't use WHERE clause to specify year and month
-----------------------------------------------------------
DROP VIEW IF EXISTS bronze.view_trip_data_green_csv
GO

CREATE VIEW bronze.view_trip_data_green_csv
AS
SELECT
    trip_data_green_csv.filepath(1) AS year,
    trip_data_green_csv.filepath(2) AS month,
    trip_data_green_csv.*
FROM
    OPENROWSET(
        BULK 'raw/trip_data_green_csv/year=*/month=*/*.csv', 
        DATA_SOURCE = 'nyc_taxi_ext_source',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) 
    WITH (
        VendorID INT,
        lpep_pickup_datetime DATETIME2(7),
        lpep_dropoff_datetime DATETIME2(7),
        store_and_fwd_flag CHAR(1),
        RatecodeID INT,
        PULocationID INT,
        DOLocationID INT,
        passenger_count INT,
        trip_distance FLOAT,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        ehail_fee INT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        payment_type INT,
        trip_type INT,
        congestion_surcharge FLOAT
    ) AS trip_data_green_csv
GO

SELECT COUNT(*) 
FROM bronze.view_trip_data_green_csv
WHERE year = 2020 AND month = 01
GO