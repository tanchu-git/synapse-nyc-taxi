USE nyc_taxi_discovery;
-----------------------------------------------------------
-- check for nulls before joining data
-----------------------------------------------------------
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS trip_data_green_parquet
WHERE PULocationID is NULL; -- query returns nothing if there are no nulls
-----------------------------------------------------------
-- joining data to see number of trips from each borough
-----------------------------------------------------------
SELECT 
    taxi_zone.borough,
    COUNT(*) AS number_of_trips
FROM 
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS trip_data
JOIN -- OPENROWSET retuns a table, just join on those 2
    OPENROWSET(
    BULK 'taxi_zone.csv',
    DATA_SOURCE = 'nyc_taxi_data_raw',
    FORMAT = 'CSV',
    FIRSTROW = 2 -- Specify starting row
    ) 
    WITH (
        location_id SMALLINT 1,
        borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone VARCHAR(15) 4
    ) AS taxi_zone
ON trip_data.PULocationID = taxi_zone.location_id
GROUP BY taxi_zone.borough
ORDER BY number_of_trips DESC;