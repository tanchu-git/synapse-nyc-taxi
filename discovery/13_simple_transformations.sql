USE nyc_taxi_discovery;
-----------------------------------------------------------
-- check trip durations in minutes and convert to hours using DATEDIFF
-----------------------------------------------------------
SELECT
    DATEDIFF(minute, lpep_pickup_datetime, lpep_dropoff_datetime) / 60 AS from_hour, -- DATEDIFF(format, starttime, endtime) 
    (DATEDIFF(minute, lpep_pickup_datetime, lpep_dropoff_datetime) / 60) + 1 AS to_hour,
    COUNT(*) AS number_of_trips
FROM 
    OPENROWSET(
        BULK 'trip_data_green_parquet/**',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS trip_data
GROUP BY DATEDIFF(minute, lpep_pickup_datetime, lpep_dropoff_datetime) / 60,
         (DATEDIFF(minute, lpep_pickup_datetime, lpep_dropoff_datetime) / 60) + 1
ORDER BY from_hour, to_hour;
