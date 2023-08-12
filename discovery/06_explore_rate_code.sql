USE nyc_taxi_discovery;
-----------------------------------------------------------
-- Standard JSON file - OPENJSON solution
-----------------------------------------------------------
SELECT rate_code_id, rate_code
    FROM OPENROWSET(
        BULK 'rate_code.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'csv',
        PARSER_VERSION = '1.0', -- default
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0b' -- vertical tab
    )
    WITH(
        jsonDoc NVARCHAR(MAX)
    )
    AS payment_type
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        rate_code_id TINYINT,
        rate_code VARCHAR(25) -- '$.rate_code' -- renamed
    );

-----------------------------------------------------------
-- Standard multiline JSON file - same as above
-----------------------------------------------------------
SELECT rate_code_id, rate_code
    FROM OPENROWSET(
        BULK 'rate_code_multi_line.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'csv',
        PARSER_VERSION = '1.0', -- default
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0b' -- vertical tab
    )
    WITH(
        jsonDoc NVARCHAR(MAX)
    )
    AS payment_type
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        rate_code_id TINYINT,
        rate_code VARCHAR(25) -- '$.rate_code' -- renamed
    );