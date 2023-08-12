USE nyc_taxi_discovery;
/*
Identify the percentage of cash and credit card trips by borough.
Common Table Expression - CTE - WITH [temp_name] AS [SELECT_clause] 
to create temporary tables with names for reference [v_temp_name]
*/
-- 1st temp table
WITH v_trip_data AS (
    SELECT *
    FROM 
        OPENROWSET(
            BULK 'trip_data_green_parquet/year=2021/month=01/',
            DATA_SOURCE = 'nyc_taxi_data_raw',
            FORMAT = 'PARQUET'
        ) 
    AS trip_data
),
-- 2nd temp table
v_taxi_zone AS (
    SELECT *
    FROM
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
),
-- 3rd temp table
v_payment_type AS (
    SELECT *
    FROM
        OPENROWSET(
            BULK 'payment_type.json',
            DATA_SOURCE = 'nyc_taxi_data_raw',
            FORMAT = 'csv',
            PARSER_VERSION = '1.0',
            FIELDTERMINATOR = '0x0b',
            FIELDQUOTE = '0x0b',
            ROWTERMINATOR = '0x0a' --\n
        )
    WITH(
        jsonDoc NVARCHAR(MAX)
    )
    AS payment_type
    CROSS APPLY OPENJSON(jsonDoc)
        WITH(
            payment_type SMALLINT,
            description VARCHAR(20) '$.payment_type_desc'
    )AS final_payment_type
)
/*
Join the temporary tables by their temporary names
Count the number of cash and card trips for each borough
Cast to convert INT (cash/card trips) to DECIMAL for division to work
DECIMAL(p, s) - p defines max number of decimal digits to be stored. This number includes both the left and the right sides of the decimal point.
                s defines max number of decimal digits to be stored to the right of the decimal point. 
                This number is subtracted from p to determine the maximum number of digits to the left of the decimal point.
*/
SELECT 
    v_taxi_zone.borough,
    COUNT(*) AS total_trips,
    COUNT(CASE WHEN v_payment_type.description = 'Cash' THEN 1 END) AS cash_trips,
    COUNT(CASE WHEN v_payment_type.description = 'Credit Card' THEN 1 END) AS card_trips,
    CAST((COUNT(CASE WHEN v_payment_type.description = 'Cash' THEN 1 END) / CAST(COUNT(*) AS DECIMAL)) * 100 AS DECIMAL(5, 2)) AS cash_percentage,
    CAST((COUNT(CASE WHEN v_payment_type.description = 'Credit Card' THEN 1 END) / CAST(COUNT(*) AS DECIMAL)) * 100 AS DECIMAL(5, 2)) AS card_percentage
    FROM v_trip_data
        LEFT JOIN v_payment_type ON v_trip_data.payment_type = v_payment_type.payment_type
        LEFT JOIN v_taxi_zone ON v_trip_data.PULocationId = v_taxi_zone.location_id
WHERE v_payment_type.payment_type in (1, 2) -- limit the payment types to cash and credit
GROUP BY v_taxi_zone.borough
ORDER BY v_taxi_zone.borough;