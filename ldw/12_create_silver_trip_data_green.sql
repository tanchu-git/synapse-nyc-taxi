USE nyc_taxi_ldw;

--create silver table for partitioned CSV trip_data_green
--will write all partitions into one single parquet file
--no more partitions to query with

--IF OBJECT_ID('silver.trip_data_green') IS NOT NULL
--    DROP EXTERNAL TABLE silver.trip_data_green
--GO

--CREATE EXTERNAL TABLE silver.trip_data_green
--    WITH (
--        LOCATION = 'silver/trip_data_green',
--        DATA_SOURCE = nyc_taxi_ext_source,
--        FILE_FORMAT = parquet_file_format
--    )
--AS
--SELECT *
--    FROM bronze.trip_data_green_csv;

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
