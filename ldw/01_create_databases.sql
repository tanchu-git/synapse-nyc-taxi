USE master
GO

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'nyc_taxi_ldw')
    CREATE DATABASE nyc_taxi_ldw
GO

ALTER DATABASE nyc_taxi_ldw COLLATE Latin1_General_100_BIN2_UTF8
GO

USE nyc_taxi_ldw
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    -- The schema must be run in its own batch!
    EXEC( 'CREATE SCHEMA bronze' );
END

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    -- The schema must be run in its own batch!
    EXEC( 'CREATE SCHEMA silver' );
END

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    -- The schema must be run in its own batch!
    EXEC( 'CREATE SCHEMA gold' );
END