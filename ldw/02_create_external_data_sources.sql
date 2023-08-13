USE nyc_taxi_ldw;

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'nyc_taxi_ext_source')
    CREATE EXTERNAL DATA SOURCE nyc_taxi_ext_source
    WITH
    (
        LOCATION = 'https://udemysynapsecourse.dfs.core.windows.net/nyc-taxi-data'
    );