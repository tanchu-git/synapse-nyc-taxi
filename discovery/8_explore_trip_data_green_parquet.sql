USE nyc_taxi_discovery;
-----------------------------------------------------------
-- Parquet files - columnar format efficiency
-----------------------------------------------------------
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) AS trip_data_green_parquet;

-- Look up the inferred data types
EXEC sp_describe_first_result_set N'
    SELECT
        TOP 100 *
    FROM
    OPENROWSET(
        BULK ''trip_data_green_parquet/year=2020/month=01/'',
        DATA_SOURCE = ''nyc_taxi_data_raw'',
        FORMAT = ''PARQUET''
    ) AS trip_data_green_parquet';

-- define data types
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
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
    AS trip_data_green_parquet;

-- fewer columns to increase poermance and reduce costs
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) 
    WITH (
        tip_amount FLOAT,
        trip_type INT
    )
    AS trip_data_green_parquet;

-----------------------------------------------------------
-- query from subfolders using wildcard characters
-- use filenamefunction
-- query from subfolders
-- use filepatch function to select only from certain partitions
-----------------------------------------------------------
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ('trip_data_green_parquet/year=2020/month=01/',
              'trip_data_green_parquet/year=2021/**'),
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) 
    WITH (
        tip_amount FLOAT,
        trip_type INT
    )
    AS trip_data_green_parquet;

-----------------------------------------------------------
SELECT
    trip_data_green_parquet.filename() AS file_name,
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK ('trip_data_green_parquet/year=2020/month=01/',
              'trip_data_green_parquet/year=2021/**'),
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) 
    WITH (
        tip_amount FLOAT,
        trip_type INT
    )    
    AS trip_data_green_parquet
    GROUP BY trip_data_green_parquet.filename();

-----------------------------------------------------------
SELECT
    trip_data_green_parquet.filename() AS file_name,
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK ('trip_data_green_parquet/year=2020/month=01/',
              'trip_data_green_parquet/year=2021/**'),
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) 
    WITH (
        tip_amount FLOAT,
        trip_type INT
    )    
    AS trip_data_green_parquet
    --WHERE trip_data_green_parquet.filename() IN (
       -- 'part-00000-tid-6133789922049958496-2e489315-890a-4453-ae93-a104be9a6f06-106-1-c000.snappy.parquet')
    GROUP BY trip_data_green_parquet.filename();

-----------------------------------------------------------
-- Need wildcard for filepath - 1: year=*, 2: month=*, 3: *filename
SELECT
    trip_data_green_parquet.filepath(1) AS year,
    trip_data_green_parquet.filepath(2) AS month,
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=*/month=*/',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'PARQUET'
    ) 
    WITH (
        tip_amount FLOAT,
        trip_type INT
    )    
    AS trip_data_green_parquet
    WHERE trip_data_green_parquet.filepath(1) IN ('2020', '2021') AND trip_data_green_parquet.filepath(2) IN ('02', '12')
    GROUP BY trip_data_green_parquet.filepath(1), trip_data_green_parquet.filepath(2)
    ORDER BY trip_data_green_parquet.filepath(1), trip_data_green_parquet.filepath(2);
