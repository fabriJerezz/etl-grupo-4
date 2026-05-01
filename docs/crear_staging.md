# Script para crear la base de datos staging (de datos crudos) en SSMS

-- =====================================================
-- STAGING AREA - Data Warehouse Tráfico Aéreo
-- Motor: SQL Server
-- Propósito: Tablas intermedias sin constraints para
--            recibir los CSVs crudos antes de transformar
-- =====================================================

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'dw_staging_raw')
BEGIN
    CREATE DATABASE dw_staging_raw;
END
GO

USE dw_staging_raw;
GO

-- ===========================================
-- Eliminar tablas si existen (re-ejecutable)
-- ===========================================
IF OBJECT_ID('dbo.stg_t100', 'U')                    IS NOT NULL DROP TABLE dbo.stg_t100;
IF OBJECT_ID('dbo.stg_unique_carriers', 'U')          IS NOT NULL DROP TABLE dbo.stg_unique_carriers;
IF OBJECT_ID('dbo.stg_carrier_history', 'U')          IS NOT NULL DROP TABLE dbo.stg_carrier_history;
IF OBJECT_ID('dbo.stg_carrier_group', 'U')            IS NOT NULL DROP TABLE dbo.stg_carrier_group;
IF OBJECT_ID('dbo.stg_carrier_group_new', 'U')        IS NOT NULL DROP TABLE dbo.stg_carrier_group_new;
IF OBJECT_ID('dbo.stg_airports', 'U')                 IS NOT NULL DROP TABLE dbo.stg_airports;
IF OBJECT_ID('dbo.stg_airport_ids', 'U')              IS NOT NULL DROP TABLE dbo.stg_airport_ids;
IF OBJECT_ID('dbo.stg_airport_seq_ids', 'U')          IS NOT NULL DROP TABLE dbo.stg_airport_seq_ids;
IF OBJECT_ID('dbo.stg_country_codes', 'U')            IS NOT NULL DROP TABLE dbo.stg_country_codes;
IF OBJECT_ID('dbo.stg_world_area_codes', 'U')         IS NOT NULL DROP TABLE dbo.stg_world_area_codes;
IF OBJECT_ID('dbo.stg_city_market_ids', 'U')          IS NOT NULL DROP TABLE dbo.stg_city_market_ids;
IF OBJECT_ID('dbo.stg_service_class', 'U')            IS NOT NULL DROP TABLE dbo.stg_service_class;
IF OBJECT_ID('dbo.stg_aircraft_type', 'U')            IS NOT NULL DROP TABLE dbo.stg_aircraft_type;
IF OBJECT_ID('dbo.stg_aircraft_group', 'U')           IS NOT NULL DROP TABLE dbo.stg_aircraft_group;
IF OBJECT_ID('dbo.stg_aircraft_config', 'U')          IS NOT NULL DROP TABLE dbo.stg_aircraft_config;
IF OBJECT_ID('dbo.stg_distance_group', 'U')           IS NOT NULL DROP TABLE dbo.stg_distance_group;
IF OBJECT_ID('dbo.stg_months', 'U')                   IS NOT NULL DROP TABLE dbo.stg_months;
IF OBJECT_ID('dbo.stg_quarters', 'U')                 IS NOT NULL DROP TABLE dbo.stg_quarters;
IF OBJECT_ID('dbo.stg_regions', 'U')                  IS NOT NULL DROP TABLE dbo.stg_regions;
IF OBJECT_ID('dbo.stg_airline_ids', 'U')              IS NOT NULL DROP TABLE dbo.stg_airline_ids;
IF OBJECT_ID('dbo.stg_unique_carrier_entities', 'U')  IS NOT NULL DROP TABLE dbo.stg_unique_carrier_entities;
GO

-- =====================================================
-- TABLA PRINCIPAL: T100 Segments (98.622 filas)
-- Todos los campos como VARCHAR/FLOAT para recibir
-- datos crudos sin restricciones
-- =====================================================
CREATE TABLE stg_t100 (
    Year                    INT,
    Quarter                 INT,
    Month                   INT,
    UniqueCarrier           VARCHAR(10),
    AirlineID               INT,
    UniqueCarrierName       VARCHAR(100),
    UniqCarrierEntity       VARCHAR(10),
    CarrierRegion           VARCHAR(20),
    Carrier                 VARCHAR(10),
    CarrierName             VARCHAR(10),
    CarrierGroup            VARCHAR(20),
    CarrierGroupNew         VARCHAR(5),
    Origin                  VARCHAR(5),
    OriginAirportID         INT,
    OriginAirportSeqID      INT,
    OriginCityMarketID      INT,
    OriginCityName          VARCHAR(100),
    OriginCountry           VARCHAR(5),
    OriginCountryName       VARCHAR(50),
    OriginWAC               INT,
    Dest                    VARCHAR(5),
    DestAirportID           INT,
    DestAirportSeqID        INT,
    DestCityMarketID        INT,
    DestCityName            VARCHAR(100),
    DestCountry             VARCHAR(5),
    DestCountryName         VARCHAR(50),
    DestWAC                 INT,
    AircraftGroup           VARCHAR(10),
    AircraftType            VARCHAR(10),
    AircraftConfig          VARCHAR(10),
    Class                   VARCHAR(5),
    DepScheduled            INT,
    DepPerformed            INT,
    Seats                   INT,
    Passengers              INT,
    Freight                 FLOAT,
    Mail                    FLOAT,
    Payload                 FLOAT,
    Distance                FLOAT,
    DistanceGroup           INT,
    RampTime                INT,
    AirTime                 INT
);
GO

-- =====================================================
-- TABLAS LOOKUP (formato Code / Description)
-- =====================================================

CREATE TABLE stg_unique_carriers (
    Code         VARCHAR(10),
    Description  VARCHAR(100)
);
GO

CREATE TABLE stg_carrier_history (
    AirlineID      VARCHAR(10),
    UniqueCarrier  VARCHAR(10),
    CarrierName    VARCHAR(100),
    StartDate      VARCHAR(20),
    EndDate        VARCHAR(20)
);
GO

CREATE TABLE stg_carrier_group (
    Code         VARCHAR(10),
    Description  VARCHAR(100)
);
GO

CREATE TABLE stg_carrier_group_new (
    Code         VARCHAR(10),
    Description  VARCHAR(100)
);
GO

CREATE TABLE stg_airports (
    Code         VARCHAR(10),
    Description  VARCHAR(200)
);
GO

CREATE TABLE stg_airport_ids (
    Code         VARCHAR(10),
    Description  VARCHAR(200)
);
GO

CREATE TABLE stg_airport_seq_ids (
    Code         VARCHAR(15),
    Description  VARCHAR(200)
);
GO

CREATE TABLE stg_country_codes (
    Code         VARCHAR(5),
    Description  VARCHAR(100)
);
GO

CREATE TABLE stg_world_area_codes (
    Code         VARCHAR(10),
    Description  VARCHAR(100)
);
GO

CREATE TABLE stg_city_market_ids (
    Code         VARCHAR(10),
    Description  VARCHAR(100)
);
GO

CREATE TABLE stg_service_class (
    Code         VARCHAR(5),
    Description  VARCHAR(100)
);
GO

CREATE TABLE stg_aircraft_type (
    Code         VARCHAR(10),
    Description  VARCHAR(100)
);
GO

CREATE TABLE stg_aircraft_group (
    Code         VARCHAR(10),
    Description  VARCHAR(100)
);
GO

CREATE TABLE stg_aircraft_config (
    Code         VARCHAR(10),
    Description  VARCHAR(100)
);
GO

CREATE TABLE stg_distance_group (
    Code         VARCHAR(10),
    Description  VARCHAR(100)
);
GO

CREATE TABLE stg_months (
    Code         VARCHAR(5),
    Description  VARCHAR(50)
);
GO

CREATE TABLE stg_quarters (
    Code         VARCHAR(5),
    Description  VARCHAR(50)
);
GO

CREATE TABLE stg_regions (
    Code         VARCHAR(10),
    Description  VARCHAR(100)
);
GO

CREATE TABLE stg_airline_ids (
    Code         VARCHAR(10),
    Description  VARCHAR(100)
);
GO

CREATE TABLE stg_unique_carrier_entities (
    Code         VARCHAR(10),
    Description  VARCHAR(100)
);
GO

PRINT '✓ Staging area creada exitosamente - 21 tablas';
GO