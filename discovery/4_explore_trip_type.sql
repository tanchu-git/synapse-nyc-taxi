USE nyc_taxi_discovery;
-----------------------------------------------------------
-- TSV file
-----------------------------------------------------------
SELECT *
    FROM OPENROWSET(
        BULK 'trip_type.tsv',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'csv',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = '\t'
    )
    WITH(
        trip_type TINYINT,
        trip_type_desc VARCHAR(15)
    )
    AS trip_type;