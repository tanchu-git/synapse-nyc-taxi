USE nyc_taxi_ldw;
-----------------------------------------------------------
-- create external file format for csv files
-----------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'csv_file_format')
    CREATE EXTERNAL FILE FORMAT csv_file_format
    WITH(
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS(
            FIELD_TERMINATOR = ',',
            STRING_DELIMITER = '"',
            FIRST_ROW = 2,
            USE_TYPE_DEFAULT = FALSE, -- null values will be replaced with a default value if set to TRUE
            ENCODING = 'UTF8',
            PARSER_VERSION = '2.0'
        )
    );
-- parser version 1.0
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'csv_file_format_pv1')
    CREATE EXTERNAL FILE FORMAT csv_file_format_pv1
    WITH(
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS(
            FIELD_TERMINATOR = ',',
            STRING_DELIMITER = '"',
            FIRST_ROW = 2,
            USE_TYPE_DEFAULT = FALSE, -- null values will be replaced with a default value if set to TRUE
            ENCODING = 'UTF8',
            PARSER_VERSION = '1.0'
        )
    );


-----------------------------------------------------------
-- create external file format for tsv files
-----------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'tsv_file_format')
    CREATE EXTERNAL FILE FORMAT tsv_file_format
    WITH(
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS(
            FIELD_TERMINATOR = '\t',
            STRING_DELIMITER = '"',
            FIRST_ROW = 2,
            USE_TYPE_DEFAULT = FALSE, -- null values will be replaced with a default value if set to TRUE
            ENCODING = 'UTF8',
            PARSER_VERSION = '2.0'
        )
    );
-- parser version 1.0
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'tsv_file_format_pv1')
    CREATE EXTERNAL FILE FORMAT tsv_file_format_pv1
    WITH(
        FORMAT_TYPE = DELIMITEDTEXT,
        FORMAT_OPTIONS(
            FIELD_TERMINATOR = '\t',
            STRING_DELIMITER = '"',
            FIRST_ROW = 2,
            USE_TYPE_DEFAULT = FALSE, -- null values will be replaced with a default value if set to TRUE
            ENCODING = 'UTF8',
            PARSER_VERSION = '1.0'
        )
    );

-----------------------------------------------------------
-- create external file format for parquet files
-----------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'parquet_file_format')
    CREATE EXTERNAL FILE FORMAT parquet_file_format
    WITH (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );

-----------------------------------------------------------
-- create external file format for delta files
-----------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'delta_file_format')
    CREATE EXTERNAL FILE FORMAT delta_file_format
    WITH (
        FORMAT_TYPE = DELTA
    );