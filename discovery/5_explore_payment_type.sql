USE nyc_taxi_discovery;
-----------------------------------------------------------
-- JSON file with 6 dictionary in it - JSON_VALUE solution
-----------------------------------------------------------
SELECT CAST(JSON_VALUE(jsonDoc, '$.payment_type') AS SMALLINT) payment_type,
        CAST(JSON_VALUE(jsonDoc, '$.payment_type_desc') AS VARCHAR(15)) payment_type_desc
    FROM OPENROWSET(
        BULK 'payment_type.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'csv',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0a' --\n - default
    )
    WITH (
        jsonDoc NVARCHAR(MAX)
    )
    AS payment_type;

-----------------------------------------------------------
-- JSON file with 6 dictionary in it - OPENJSON solution
-----------------------------------------------------------
SELECT payment_type, description
    FROM OPENROWSET(
        BULK 'payment_type.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'csv',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0a' --\n - default
    )
    WITH(
        jsonDoc NVARCHAR(MAX)
    )
    AS payment_type
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        payment_type SMALLINT,
        description VARCHAR(20) '$.payment_type_desc' -- renaming
    )AS final_payment_type;

-----------------------------------------------------------
-- Reading data from JSON with arrays/nested structure - JSON_VALUE solution
-----------------------------------------------------------
SELECT CAST(JSON_VALUE(jsonDoc, '$.payment_type') AS SMALLINT) payment_type,
       CAST(JSON_VALUE(jsonDoc, '$.payment_type_desc[0].value') AS VARCHAR(15)) payment_type_desc_0, -- .loc indexing
       CAST(JSON_VALUE(jsonDoc, '$.payment_type_desc[1].value') AS VARCHAR(15)) payment_type_desc_1  -- to reach nested array
    FROM OPENROWSET(
        BULK 'payment_type_array.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'csv',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0a' --\n - default
    )
    WITH (
        jsonDoc NVARCHAR(MAX)
    )
    AS payment_type;

-----------------------------------------------------------
-- Reading data from JSON with arrays/nested structure - OPENJSON solution
-----------------------------------------------------------
SELECT payment_type, payment_type_desc_value
    FROM OPENROWSET(
        BULK 'payment_type_array.json',
        DATA_SOURCE = 'nyc_taxi_data_raw',
        FORMAT = 'csv',
        PARSER_VERSION = '1.0',
        FIELDTERMINATOR = '0x0b',
        FIELDQUOTE = '0x0b',
        ROWTERMINATOR = '0x0a' --\n - default
    )
    WITH(
        jsonDoc NVARCHAR(MAX)
    )
    AS payment_type
    CROSS APPLY OPENJSON(jsonDoc)
    WITH(
        payment_type SMALLINT,
        payment_type_desc NVARCHAR(MAX) AS JSON -- read the sub array "value" as another JSON file
    )
    CROSS APPLY OPENJSON(payment_type_desc)
    WITH(
        sub_type SMALLINT,
        payment_type_desc_value VARCHAR(20) '$.value' -- cross apply the new JSON file
    );