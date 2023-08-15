/*
Use COPY INTO to load data from azure data lake storage into dedicated sql pool internal table
*/

COPY INTO dwh.trip_data_green_agg_copy_into
FROM 'https://udemysynapsecourse.dfs.core.windows.net/nyc-taxi-data/gold/trip_data_green_agg'
WITH
(
	FILE_TYPE = 'PARQUET',
	MAXERRORS = 0,
	COMPRESSION = 'snappy',
	AUTO_CREATE_TABLE = 'on'
)
GO

SELECT TOP 100 * FROM dwh.trip_data_green_agg_copy_into
GO