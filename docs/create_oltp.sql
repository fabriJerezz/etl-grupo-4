-- ============================================================
-- CREACION BASE DE DATOS OLTP DESTINO
-- Proyecto : Data Warehouse - Tráfico Aéreo T100 2023
-- Motor    : SQL Server
-- Propósito: Modelo OLTP normalizado. Destino final del ETL.
-- EJECUCION: Correr una sola vez antes del primer transform_load.sql
-- ============================================================

-- ============================================================
-- 0. CREAR BASE DE DATOS
-- ============================================================
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'dw_oltp')
BEGIN
    CREATE DATABASE dw_oltp;
    PRINT '✓ Base de datos dw_oltp creada.';
END
ELSE
    PRINT '  Base de datos dw_oltp ya existe. Continuando...';
GO

USE dw_oltp;
GO

SET NOCOUNT ON;
GO

-- ============================================================
-- 1. ELIMINAR TABLAS EN ORDEN INVERSO DE DEPENDENCIA
--    (para permitir re-ejecucion del script)
-- ============================================================
IF OBJECT_ID('dbo.SEGMENTOS_VUELO',        'U') IS NOT NULL DROP TABLE dbo.SEGMENTOS_VUELO;
IF OBJECT_ID('dbo.HISTORIAL_TRANSPORTISTA','U') IS NOT NULL DROP TABLE dbo.HISTORIAL_TRANSPORTISTA;
IF OBJECT_ID('dbo.TRANSPORTISTAS',         'U') IS NOT NULL DROP TABLE dbo.TRANSPORTISTAS;
IF OBJECT_ID('dbo.GRUPOS_TRANSPORTISTA',   'U') IS NOT NULL DROP TABLE dbo.GRUPOS_TRANSPORTISTA;
IF OBJECT_ID('dbo.AEROPUERTOS',            'U') IS NOT NULL DROP TABLE dbo.AEROPUERTOS;
IF OBJECT_ID('dbo.REGIONES_WAC',           'U') IS NOT NULL DROP TABLE dbo.REGIONES_WAC;
IF OBJECT_ID('dbo.PAISES',                 'U') IS NOT NULL DROP TABLE dbo.PAISES;
IF OBJECT_ID('dbo.TIPOS_AERONAVE',         'U') IS NOT NULL DROP TABLE dbo.TIPOS_AERONAVE;
IF OBJECT_ID('dbo.CLASES_SERVICIO',        'U') IS NOT NULL DROP TABLE dbo.CLASES_SERVICIO;
IF OBJECT_ID('dbo.GRUPOS_DISTANCIA',       'U') IS NOT NULL DROP TABLE dbo.GRUPOS_DISTANCIA;
PRINT '✓ Tablas previas eliminadas (si existían).';
GO

-- ============================================================
-- 2. TABLAS DE REFERENCIA INDEPENDIENTES
--    No tienen FK hacia otras tablas del modelo.
-- ============================================================

-- 2a. PAISES
--     Fuente: stg_country_codes
CREATE TABLE dbo.PAISES (
    PaisID      INT           NOT NULL IDENTITY(1,1),
    CodigoPais  CHAR(5)       NOT NULL,   -- Código ISO / BTS (ej. "US", "MX")
    NombrePais  VARCHAR(100)  NOT NULL,
    CONSTRAINT PK_PAISES PRIMARY KEY (PaisID),
    CONSTRAINT UQ_PAISES_Codigo UNIQUE (CodigoPais)
);
GO

-- 2b. REGIONES_WAC
--     World Area Codes - clasificación geográfica del BTS
--     Fuente: stg_world_area_codes
CREATE TABLE dbo.REGIONES_WAC (
    WACID          INT        NOT NULL IDENTITY(1,1),
    CodigoWAC      SMALLINT   NOT NULL,   -- Código numérico BTS (1-999)
    DescripcionWAC VARCHAR(100) NOT NULL,
    CONSTRAINT PK_REGIONES_WAC PRIMARY KEY (WACID),
    CONSTRAINT UQ_REGIONES_WAC_Codigo UNIQUE (CodigoWAC)
);
GO

-- 2c. TIPOS_AERONAVE
--     Combinación de grupo + tipo + configuración de aeronave
--     Fuente: stg_aircraft_group / stg_aircraft_type / stg_aircraft_config
CREATE TABLE dbo.TIPOS_AERONAVE (
    TipoAeronaveID  INT          NOT NULL IDENTITY(1,1),
    CodigoGrupo     VARCHAR(10)  NOT NULL,   -- AircraftGroup (ej. "1")
    CodigoTipo      VARCHAR(10)  NOT NULL,   -- AircraftType  (ej. "830")
    Configuracion   VARCHAR(10)  NOT NULL,   -- AircraftConfig (ej. "1")
    Descripcion     VARCHAR(200) NULL,
    CONSTRAINT PK_TIPOS_AERONAVE PRIMARY KEY (TipoAeronaveID),
    CONSTRAINT UQ_TIPOS_AERONAVE UNIQUE (CodigoGrupo, CodigoTipo, Configuracion)
);
GO

-- 2d. CLASES_SERVICIO
--     Tipo de servicio del vuelo (Passenger, Freight, Mail, etc.)
--     Fuente: stg_service_class
CREATE TABLE dbo.CLASES_SERVICIO (
    ClaseServicioID  INT        NOT NULL IDENTITY(1,1),
    CodigoClase      CHAR(5)    NOT NULL,   -- Código BTS (ej. "F", "G", "P")
    DescripcionClase VARCHAR(100) NOT NULL,
    CONSTRAINT PK_CLASES_SERVICIO PRIMARY KEY (ClaseServicioID),
    CONSTRAINT UQ_CLASES_SERVICIO_Codigo UNIQUE (CodigoClase)
);
GO

-- 2e. GRUPOS_DISTANCIA
--     Agrupación de distancias en rangos de 500 millas
--     Fuente: stg_distance_group
CREATE TABLE dbo.GRUPOS_DISTANCIA (
    GrupoDistanciaID  INT          NOT NULL IDENTITY(1,1),
    CodigoGrupo       SMALLINT     NOT NULL,   -- 1 = 0-499 mi, 2 = 500-999 mi, etc.
    DescripcionGrupo  VARCHAR(100) NOT NULL,
    CONSTRAINT PK_GRUPOS_DISTANCIA PRIMARY KEY (GrupoDistanciaID),
    CONSTRAINT UQ_GRUPOS_DISTANCIA_Codigo UNIQUE (CodigoGrupo)
);
GO

-- ============================================================
-- 3. AEROPUERTOS
--    Depende de: PAISES (PaisID), REGIONES_WAC (WACID)
--    Clave natural: AirportSeqID (incluye período de vigencia)
--    Fuente: stg_airports + stg_t100 (columnas de origen/destino)
-- ============================================================
CREATE TABLE dbo.AEROPUERTOS (
    AeropuertoID     INT          NOT NULL IDENTITY(1,1),
    AirportSeqID     INT          NOT NULL,   -- Clave BTS con período (ej. 1000101)
    AirportID        INT          NULL,        -- ID base del aeropuerto
    CityMarketID     INT          NULL,        -- ID del mercado urbano
    CodigoIATA       CHAR(5)      NOT NULL,   -- Código IATA (ej. "JFK", "LAX")
    NombreAeropuerto VARCHAR(200) NULL,
    Ciudad           VARCHAR(100) NULL,
    PaisID           INT          NULL,
    WACID            INT          NULL,
    FechaDesde       DATE         NULL,
    FechaHasta       DATE         NULL,
    CONSTRAINT PK_AEROPUERTOS PRIMARY KEY (AeropuertoID),
    CONSTRAINT UQ_AEROPUERTOS_SeqID UNIQUE (AirportSeqID),
    CONSTRAINT FK_AEROPUERTOS_PAISES
        FOREIGN KEY (PaisID) REFERENCES dbo.PAISES (PaisID),
    CONSTRAINT FK_AEROPUERTOS_WAC
        FOREIGN KEY (WACID) REFERENCES dbo.REGIONES_WAC (WACID)
);
GO

-- Índices de búsqueda frecuente
CREATE INDEX IX_AEROPUERTOS_IATA     ON dbo.AEROPUERTOS (CodigoIATA);
CREATE INDEX IX_AEROPUERTOS_AirportID ON dbo.AEROPUERTOS (AirportID);
GO

-- ============================================================
-- 4. GRUPOS_TRANSPORTISTA
--    Clasificación del carrier (Major, National, Regional, etc.)
--    Fuente: stg_carrier_group_new / stg_carrier_group
-- ============================================================
CREATE TABLE dbo.GRUPOS_TRANSPORTISTA (
    GrupoTransportistaID  INT          NOT NULL IDENTITY(1,1),
    CodigoGrupo           VARCHAR(10)  NOT NULL,
    NombreGrupo           VARCHAR(100) NOT NULL,
    CONSTRAINT PK_GRUPOS_TRANSPORTISTA PRIMARY KEY (GrupoTransportistaID),
    CONSTRAINT UQ_GRUPOS_TRANSPORTISTA_Codigo UNIQUE (CodigoGrupo)
);
GO

-- ============================================================
-- 5. TRANSPORTISTAS
--    Depende de: GRUPOS_TRANSPORTISTA (GrupoTransportistaID)
--    Clave natural: AirlineID (clave estable DOT/BTS)
--    Fuente: stg_t100 + stg_unique_carriers + stg_carrier_history
-- ============================================================
CREATE TABLE dbo.TRANSPORTISTAS (
    TransportistaID      INT          NOT NULL IDENTITY(1,1),
    AirlineID            INT          NOT NULL,   -- ID numérico estable del BTS
    UniqueCarrierCode    VARCHAR(10)  NOT NULL,   -- Código IATA limpio (sin sufijo)
    UniqueCarrierName    VARCHAR(200) NOT NULL,   -- Nombre normalizado (UPPER, sin Inc.)
    GrupoTransportistaID INT          NULL,
    Activo               BIT          NOT NULL DEFAULT 1,   -- 0 = fusionado/cerrado
    FechaDesde           DATE         NULL,
    FechaHasta           DATE         NULL,
    CONSTRAINT PK_TRANSPORTISTAS PRIMARY KEY (TransportistaID),
    CONSTRAINT UQ_TRANSPORTISTAS_AirlineID UNIQUE (AirlineID),
    CONSTRAINT FK_TRANSPORTISTAS_GRUPO
        FOREIGN KEY (GrupoTransportistaID)
        REFERENCES dbo.GRUPOS_TRANSPORTISTA (GrupoTransportistaID)
);
GO

CREATE INDEX IX_TRANSPORTISTAS_Codigo ON dbo.TRANSPORTISTAS (UniqueCarrierCode);
GO

-- ============================================================
-- 6. HISTORIAL_TRANSPORTISTA
--    Depende de: TRANSPORTISTAS (TransportistaID)
--    Registra cambios de código IATA por fusiones/adquisiciones
--    Fuente: stg_carrier_history
-- ============================================================
CREATE TABLE dbo.HISTORIAL_TRANSPORTISTA (
    HistorialID      INT          NOT NULL IDENTITY(1,1),
    TransportistaID  INT          NOT NULL,
    CodigoAnterior   VARCHAR(10)  NOT NULL,   -- Código IATA antes del cambio
    CodigoNuevo      VARCHAR(10)  NOT NULL,   -- Código IATA después del cambio
    FechaCambio      DATE         NULL,
    CONSTRAINT PK_HISTORIAL_TRANSPORTISTA PRIMARY KEY (HistorialID),
    CONSTRAINT FK_HISTORIAL_TRANSPORTISTAS
        FOREIGN KEY (TransportistaID) REFERENCES dbo.TRANSPORTISTAS (TransportistaID)
);
GO

-- ============================================================
-- 7. SEGMENTOS_VUELO  (tabla de hechos)
--    Depende de todas las dimensiones anteriores.
--    Granularidad: un segmento = una ruta + carrier + período + clase
--    Fuente: stg_t100 (tras todas las reglas de calidad)
-- ============================================================
CREATE TABLE dbo.SEGMENTOS_VUELO (
    SegmentoID              BIGINT   NOT NULL IDENTITY(1,1),
    -- Tiempo
    Anio                    SMALLINT NOT NULL,
    Trimestre               TINYINT  NOT NULL,
    Mes                     TINYINT  NOT NULL,
    -- Claves foráneas a dimensiones
    TransportistaID         INT      NOT NULL,
    OrigenAeropuertoID      INT      NOT NULL,
    DestinoAeropuertoID     INT      NOT NULL,
    TipoAeronaveID          INT      NULL,
    ClaseServicioID         INT      NULL,
    GrupoDistanciaID        INT      NULL,
    -- Métricas operacionales
    VuelosProgramados       INT      NOT NULL DEFAULT 0,
    VuelosRealizados        INT      NOT NULL DEFAULT 0,
    AsientosDisponibles     INT      NOT NULL DEFAULT 0,
    PasajerosTransportados  INT      NOT NULL DEFAULT 0,
    CargaLibras             DECIMAL(18,2) NOT NULL DEFAULT 0,
    CorreoLibras            DECIMAL(18,2) NOT NULL DEFAULT 0,
    DistanciaMillas         DECIMAL(10,2) NOT NULL DEFAULT 0,
    TiempoVuelo             INT      NOT NULL DEFAULT 0,   -- en minutos
    -- Auditoría
    FechaCarga              DATETIME NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_SEGMENTOS_VUELO PRIMARY KEY (SegmentoID),
    CONSTRAINT FK_SV_TRANSPORTISTA
        FOREIGN KEY (TransportistaID)
        REFERENCES dbo.TRANSPORTISTAS (TransportistaID),
    CONSTRAINT FK_SV_AEROPUERTO_ORIGEN
        FOREIGN KEY (OrigenAeropuertoID)
        REFERENCES dbo.AEROPUERTOS (AeropuertoID),
    CONSTRAINT FK_SV_AEROPUERTO_DESTINO
        FOREIGN KEY (DestinoAeropuertoID)
        REFERENCES dbo.AEROPUERTOS (AeropuertoID),
    CONSTRAINT FK_SV_TIPO_AERONAVE
        FOREIGN KEY (TipoAeronaveID)
        REFERENCES dbo.TIPOS_AERONAVE (TipoAeronaveID),
    CONSTRAINT FK_SV_CLASE_SERVICIO
        FOREIGN KEY (ClaseServicioID)
        REFERENCES dbo.CLASES_SERVICIO (ClaseServicioID),
    CONSTRAINT FK_SV_GRUPO_DISTANCIA
        FOREIGN KEY (GrupoDistanciaID)
        REFERENCES dbo.GRUPOS_DISTANCIA (GrupoDistanciaID),
    -- Restricciones de negocio (última línea de defensa)
    CONSTRAINT CK_SV_Trimestre
        CHECK (Trimestre BETWEEN 1 AND 4),
    CONSTRAINT CK_SV_Mes
        CHECK (Mes BETWEEN 1 AND 12),
    CONSTRAINT CK_SV_VuelosRealizados
        CHECK (VuelosRealizados <= VuelosProgramados),
    CONSTRAINT CK_SV_Pasajeros
        CHECK (PasajerosTransportados <= AsientosDisponibles),
    CONSTRAINT CK_SV_Distancia
        CHECK (DistanciaMillas > 0),
    CONSTRAINT CK_SV_TiempoVuelo
        CHECK (TiempoVuelo > 0 AND TiempoVuelo <= 1200),
    CONSTRAINT CK_SV_Carga
        CHECK (CargaLibras >= 0)
);
GO

-- Índices para consultas analíticas frecuentes
CREATE INDEX IX_SV_Tiempo
    ON dbo.SEGMENTOS_VUELO (Anio, Mes, Trimestre);

CREATE INDEX IX_SV_Transportista
    ON dbo.SEGMENTOS_VUELO (TransportistaID);

CREATE INDEX IX_SV_Ruta
    ON dbo.SEGMENTOS_VUELO (OrigenAeropuertoID, DestinoAeropuertoID);

CREATE INDEX IX_SV_TiempoTransportista
    ON dbo.SEGMENTOS_VUELO (Anio, Mes, TransportistaID)
    INCLUDE (PasajerosTransportados, VuelosRealizados, DistanciaMillas);
GO

-- ============================================================
-- 8. VERIFICACION: listar tablas y columnas creadas
-- ============================================================
SELECT
    t.name                                AS Tabla,
    c.column_id                           AS Orden,
    c.name                                AS Columna,
    tp.name                               AS Tipo,
    CASE WHEN c.is_nullable = 1 THEN 'NULL' ELSE 'NOT NULL' END AS Nulabilidad,
    CASE WHEN c.is_identity  = 1 THEN 'IDENTITY' ELSE '' END     AS EsIdentity
FROM sys.tables t
JOIN sys.columns c  ON c.object_id = t.object_id
JOIN sys.types   tp ON tp.user_type_id = c.user_type_id
WHERE t.schema_id = SCHEMA_ID('dbo')
ORDER BY t.name, c.column_id;

PRINT '';
PRINT '✓ Modelo OLTP dw_oltp creado exitosamente.';
PRINT '  Tablas: PAISES, REGIONES_WAC, AEROPUERTOS, GRUPOS_TRANSPORTISTA,';
PRINT '          TRANSPORTISTAS, HISTORIAL_TRANSPORTISTA, TIPOS_AERONAVE,';
PRINT '          CLASES_SERVICIO, GRUPOS_DISTANCIA, SEGMENTOS_VUELO';
GO
