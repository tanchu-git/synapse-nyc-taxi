USE nyc_taxi_discovery;
-----------------------------------------------------------
-- Check for duplicates
-----------------------------------------------------------
SELECT
    location_id,
    COUNT(*) AS number_of_records
FROM
    OPENROWSET(
        BULK 'taxi_zone.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIRSTROW = 2, -- Specify starting row
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        location_id SMALLINT 1,
        borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone VARCHAR(15) 4
    )
AS taxi_zone
GROUP BY location_id
HAVING COUNT(*) > 1; -- query returning nothing means no duplicates

-----------------------------------------------------------
SELECT
    borough,
    COUNT(*) AS number_of_records
FROM
    OPENROWSET(
        BULK 'taxi_zone.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIRSTROW = 2, -- Specify starting row
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH (
        location_id SMALLINT 1,
        borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone VARCHAR(15) 4
    )
AS taxi_zone
GROUP BY borough
HAVING COUNT(*) > 1; -- query shows the number of duplicates