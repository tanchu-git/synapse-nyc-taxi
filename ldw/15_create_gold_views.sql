USE nyc_taxi_ldw;
-----------------------------------------------------------
-- Use VIEW to prune by partition in the silver folder
-- External table with serverless sql pool can't prune 
-- with partitioned dataset, use VIEW
-----------------------------------------------------------
DROP VIEW IF EXISTS gold.view_trip_data_green
GO

CREATE VIEW gold.view_trip_data_green
AS
SELECT
    gold_trip_data_green.filepath(1) AS year,
    gold_trip_data_green.filepath(2) AS month,
    gold_trip_data_green.*
FROM
    OPENROWSET(
        BULK 'gold/trip_data_green/year=*/month=*/*.parquet', 
        DATA_SOURCE = 'nyc_taxi_ext_source',
        FORMAT = 'PARQUET'
    ) 
    WITH (
        borough VARCHAR(15),
        trip_date DATE,
        trip_day VARCHAR(10),
        is_weekend VARCHAR(1),
        card_trip_count INT,
        cash_trip_count INT,
        dispatch_trip_count INT,
        street_hail_trip_count INT,
        fare_amount FLOAT,
        trip_distance FLOAT,
        trip_duration INT
    ) AS gold_trip_data_green
GO

SELECT *
FROM gold.view_trip_data_green
--WHERE year = 2020 AND month = 03
GO