USE nyc_taxi_discovery;
-----------------------------------------------------------
-- Identify any data quality issues
-----------------------------------------------------------
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS trip_data_green_parquet;
-----------------------------------------------------------
-- Check MIN(), MAX(), AVG() and for null records
-----------------------------------------------------------
SELECT
    MIN(total_amount) AS min_total_amount,
    MAX(total_amount) AS max_total_amount,
    AVG(total_amount) AS avg_total_amount,
    COUNT(*) AS total_number_of_records, -- all records, including nulls
    COUNT(total_amount) AS not_null_total_number_of_records -- all records excluding nulls
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS trip_data_green_parquet;
-----------------------------------------------------------
-- min amount is negative, investigate
-----------------------------------------------------------
SELECT
    payment_type,
    COUNT(*) AS number_of_records
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS trip_data_green_parquet
    --WHERE total_amount < 0
    GROUP BY payment_type
    ORDER BY payment_type;
-----------------------------------------------------------
-- payment_type has many null values, convert to something appropriate according to the need
-----------------------------------------------------------