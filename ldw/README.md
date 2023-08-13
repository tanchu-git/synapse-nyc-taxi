### There are some tables created with partitions in mind, using [```STORED PROCEDURE```](https://github.com/tanchu-git/synapse_nyc_taxi/tree/main/ldw/stored_procedure).
Example - 
```sql
USE nyc_taxi_ldw
GO

CREATE OR ALTER PROCEDURE silver.sp_silver_trip_data_green
    @year VARCHAR(4),
    @month VARCHAR(2)
AS
BEGIN
    DECLARE @create_sql_statement NVARCHAR(MAX),
            @drop_sql_statement NVARCHAR(MAX);

    SET @create_sql_statement =
        'CREATE EXTERNAL TABLE silver.trip_data_green_' + @year + '_' + @month + 
        '    WITH (
                DATA_SOURCE = nyc_taxi_ext_source,
                LOCATION = ''silver/trip_data_green/year=' + @year + '/month=' + @month + ''',
                FILE_FORMAT = parquet_file_format
            )
        AS
        SELECT 
            VendorID AS vendor_id,
            lpep_pickup_datetime,
            lpep_dropoff_datetime,
            store_and_fwd_flag,
            RatecodeID AS rate_code_id,
            PULocationID AS pu_location_id,
            DOLocationID AS do_location_id,
            passenger_count,
            trip_distance,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            ehail_fee,
            improvement_surcharge,
            total_amount,
            payment_type,
            trip_type,
            congestion_surcharge
        FROM bronze.view_trip_data_green_csv
        WHERE year = ''' + @year + ''' AND month = ''' + @month + '''';
    
    SET @drop_sql_statement =
        'DROP EXTERNAL TABLE silver.trip_data_green_' + @year + '_' + @month;

    EXEC sp_executesql @create_sql_statement;
    EXEC sp_executesql @drop_sql_statement;
END;
```
And the execution.
```sql
-----------------------------------------------------------
-- execute the stored procedure to tranform into parquet files with partitions
-----------------------------------------------------------
EXEC silver.sp_silver_trip_data_green '2020', '01';
EXEC silver.sp_silver_trip_data_green '2020', '02';
EXEC silver.sp_silver_trip_data_green '2020', '03';
EXEC silver.sp_silver_trip_data_green '2020', '04';
EXEC silver.sp_silver_trip_data_green '2020', '05';
EXEC silver.sp_silver_trip_data_green '2020', '06';
EXEC silver.sp_silver_trip_data_green '2020', '07';
EXEC silver.sp_silver_trip_data_green '2020', '08';
EXEC silver.sp_silver_trip_data_green '2020', '09';
EXEC silver.sp_silver_trip_data_green '2020', '10';
EXEC silver.sp_silver_trip_data_green '2020', '11';
EXEC silver.sp_silver_trip_data_green '2020', '12';
EXEC silver.sp_silver_trip_data_green '2021', '01';
EXEC silver.sp_silver_trip_data_green '2021', '02';
EXEC silver.sp_silver_trip_data_green '2021', '03';
EXEC silver.sp_silver_trip_data_green '2021', '04';
EXEC silver.sp_silver_trip_data_green '2021', '05';
EXEC silver.sp_silver_trip_data_green '2021', '06';
```
