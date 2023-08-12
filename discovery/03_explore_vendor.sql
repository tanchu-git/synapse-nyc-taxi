USE nyc_taxi_discovery;
-----------------------------------------------------------
-- sees comma as NOT part of dataset
-----------------------------------------------------------
SELECT *
    FROM OPENROWSET(
        BULK 'vendor_unquoted.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'csv',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    )
    AS vendor;
-----------------------------------------------------------
-- Using \ -escape in source file to solve it
-----------------------------------------------------------
SELECT *
    FROM OPENROWSET(
        BULK 'vendor_escaped.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'csv',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        ESCAPECHAR = '\\'
    )
    AS vendor;
-----------------------------------------------------------
-- Using " " -quotes in source file to solve it
-----------------------------------------------------------
SELECT *
    FROM OPENROWSET(
        BULK 'vendor.csv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'csv',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDQUOTE = '"'
    )
    AS vendor;