USE nyc_taxi_discovery;

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'calendar.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        HEADER_ROW = TRUE,
        PARSER_VERSION = '2.0'
    ) 
AS calendar;

EXEC sp_describe_first_result_set N'
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ''calendar.csv'',
        DATA_SOURCE = ''nyc_taxi_data_raw'',
        FORMAT = ''CSV'',
        HEADER_ROW = TRUE,
        PARSER_VERSION = ''2.0''
    ) 
AS calendar;'


SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'calendar.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'CSV',
        HEADER_ROW = TRUE,
        PARSER_VERSION = '2.0'
    ) 
WITH(
    date_key INT,
    date DATE,
    year SMALLINT,
    month TINYINT,
    day TINYINT,
    day_name VARCHAR(10),
    day_of_year SMALLINT,
    week_of_month TINYINT,
    week_of_year TINYINT,
    month_name VARCHAR(10),
    year_month INT,
    year_week INT
)
AS calendar;

-- Check collation
SELECT name, collation_name FROM sys.databases;