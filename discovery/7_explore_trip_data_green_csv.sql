USE nyc_taxi_discovery;
-----------------------------------------------------------
-- Folder and Subfolders - month folder
-----------------------------------------------------------
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=2020/month=01/*.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS trip_data_green_csv

-----------------------------------------------------------
-- Folder and Subfolders - year folder
-----------------------------------------------------------
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=2020/**',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS trip_data_green_csv

-----------------------------------------------------------
-- Folder and Subfolders - multiple subfolders
-----------------------------------------------------------
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ('trip_data_green_csv/year=2020/month=02/*.csv',
              'trip_data_green_csv/year=2020/month=04/*.csv'),
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS trip_data_green_csv
    WHERE month(lpep_pickup_datetime) = 04
    ORDER BY lpep_pickup_datetime DESC;

-----------------------------------------------------------
-- Folder and Subfolders - multiple wildcard characters **
-----------------------------------------------------------
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=02/*.csv', -- every year folder, only 02 month, every csv file in month folder
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS trip_data_green_csv
    ORDER BY lpep_pickup_datetime DESC;

-----------------------------------------------------------
-- File metadata function filename()
-----------------------------------------------------------
SELECT
    TOP 100 
    trip_data_green_csv.filename() AS file_name, -- add column for source of data
    trip_data_green_csv.*
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=02/*.csv', 
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS trip_data_green_csv
    ORDER BY lpep_pickup_datetime DESC;

-- group by to see record count for each source file
SELECT
    trip_data_green_csv.filename() AS file_name, -- add column for source of data
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=02/*.csv', 
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS trip_data_green_csv
    GROUP BY trip_data_green_csv.filename()
    ORDER BY trip_data_green_csv.filename() DESC;

-- Limit data using filename()
SELECT
    trip_data_green_csv.filename() AS file_name, -- add column for source of data
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=02/*.csv', 
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS trip_data_green_csv
    WHERE trip_data_green_csv.filename() IN ('green_tripdata_2021-02.csv')
    GROUP BY trip_data_green_csv.filename()
    ORDER BY trip_data_green_csv.filename() DESC;

-----------------------------------------------------------
-- Use filepath() function
-----------------------------------------------------------
SELECT
    trip_data_green_csv.filename() AS file_name,
    trip_data_green_csv.filepath() AS file_path, -- get the path
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=02/*.csv', 
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS trip_data_green_csv
    WHERE trip_data_green_csv.filename() IN ('green_tripdata_2021-02.csv')
    GROUP BY trip_data_green_csv.filename(), trip_data_green_csv.filepath()
    ORDER BY trip_data_green_csv.filename(), trip_data_green_csv.filepath() DESC;

-- Specify the folders that are used, 1 for year folder, 2 for month folder - to see how many records is in each month
SELECT
    trip_data_green_csv.filepath(1) AS year, -- 1st folder is year
    trip_data_green_csv.filepath(2) AS month, -- 2nd folder is month
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*.csv', 
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS trip_data_green_csv
    GROUP BY trip_data_green_csv.filepath(1), trip_data_green_csv.filepath(2)
    ORDER BY trip_data_green_csv.filepath(1), trip_data_green_csv.filepath(2) DESC;

-- File path in WHERE clause to look for specific year and months of record counts
SELECT
    trip_data_green_csv.filepath(1) AS year,
    trip_data_green_csv.filepath(2) AS month,
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*.csv', 
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS trip_data_green_csv
    WHERE trip_data_green_csv.filepath(1) = '2020' AND trip_data_green_csv.filepath(2) IN ('06', '08')
    GROUP BY trip_data_green_csv.filepath(1), trip_data_green_csv.filepath(2)
    ORDER BY trip_data_green_csv.filepath(1), trip_data_green_csv.filepath(2) DESC;