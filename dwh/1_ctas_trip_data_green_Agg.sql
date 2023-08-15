/*
Create external table - first the schema, then the file format and datasource
Dedicated sql pool is using polybase (pararellism work) to load from azure datalake storage
Using CTAS to create table into dedicated sql pool internal table
*/
CREATE SCHEMA staging
GO

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'parquet_file_format') 
	CREATE EXTERNAL FILE FORMAT parquet_file_format
	WITH ( FORMAT_TYPE = PARQUET)
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'nyc_taxi_ext_source') 
	CREATE EXTERNAL DATA SOURCE nyc_taxi_ext_source 
	WITH (
		LOCATION = 'abfss://nyc-taxi-data@udemysynapsecourse.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE staging.external_trip_data_green_agg (
	[pu_location_id] int,
	[do_location_id] int,
	[total_trip_count] bigint,
	[total_fare_amount] float
	)
	WITH (
	LOCATION = 'gold/trip_data_green_agg',
	DATA_SOURCE = nyc_taxi_ext_source,
	FILE_FORMAT = parquet_file_format
	)
GO


SELECT TOP 100 * FROM staging.external_trip_data_green_agg
GO

CREATE SCHEMA dwh
GO

CREATE TABLE dwh.trip_data_green_agg
WITH(
	CLUSTERED COLUMNSTORE INDEX,
	DISTRIBUTION = ROUND_ROBIN
)
AS 
SELECT * FROM staging.external_trip_data_green_agg
GO

SELECT * FROM dwh.trip_data_green_agg;