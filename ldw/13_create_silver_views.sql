USE nyc_taxi_ldw;
-----------------------------------------------------------
-- Use view to prune by partition in the silver folder
-----------------------------------------------------------
DROP VIEW IF EXISTS silver.view_trip_data_green
GO

CREATE VIEW silver.view_trip_data_green
AS
SELECT
    silver_trip_data_green.filepath(1) AS year,
    silver_trip_data_green.filepath(2) AS month,
    silver_trip_data_green.*
FROM
    OPENROWSET(
        BULK 'silver/trip_data_green/year=*/month=*/*.parquet', 
        DATA_SOURCE = 'nyc_taxi_ext_source',
        FORMAT = 'PARQUET'
    ) 
    WITH (
        vendor_id INT,
        lpep_pickup_datetime DATETIME2(7),
        lpep_dropoff_datetime DATETIME2(7),
        store_and_fwd_flag CHAR(1),
        rate_code_id INT,
        pu_location_id INT,
        do_location_id INT,
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
    ) AS silver_trip_data_green
GO

SELECT TOP 100 *
FROM silver.view_trip_data_green
WHERE year = 2021 AND month = 01
GO