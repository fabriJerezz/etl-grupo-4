-- ============================================================
-- PIPELINE ETL - ETAPA TRANSFORMACION Y CARGA (T + L)
-- Origen  : dw_staging_raw  (tablas stg_*)
-- Destino : modelo OLTP normalizado
-- Motor   : SQL Server (T-SQL)
-- Autor   : Data Engineering
-- ============================================================
-- INDICE DE REGLAS DE CALIDAD IMPLEMENTADAS
--   R01  AirlineID como clave estable  (TRANSPORTISTAS)
--   R02  Normalizacion UniqueCarrierName (TRANSPORTISTAS)
--   R03  AirlineID = 0 / NULL => "Desconocido" (TRANSPORTISTAS)
--   R04  DepPerformed > DepScheduled => descarte (SEGMENTOS_VUELO)
--   R05  Passengers > Seats / Seats=0 con pax => descarte (SEGMENTOS_VUELO)
--   R06  Distance <= 0 => descarte (SEGMENTOS_VUELO)
--   R07  AirTime <= 0 o > 1200 => descarte (SEGMENTOS_VUELO)
--   R08  Freight > Payload o Freight < 0 => descarte (SEGMENTOS_VUELO)
--   R09  Uso de SeqID correcto para aeropuertos (AEROPUERTOS / SEGMENTOS_VUELO)
--   R10  Deduplicacion con ROW_NUMBER() (CTE base SEGMENTOS_VUELO)
--   R11  Calculo de Activo cruzando stg_carrier_history (TRANSPORTISTAS)
-- ============================================================

USE dw_oltp;   -- <- ajustar al nombre real de la BD destino
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

-- ============================================================
-- 0. HELPER: funcion inline para limpiar nombres de carrier
--    Resuelve R02 (normalizacion de UniqueCarrierName)
-- ============================================================
IF OBJECT_ID('dbo.fn_NormalizarNombreCarrier', 'IF') IS NOT NULL
    DROP FUNCTION dbo.fn_NormalizarNombreCarrier;
GO

CREATE FUNCTION dbo.fn_NormalizarNombreCarrier (@nombre VARCHAR(200))
RETURNS TABLE
AS
RETURN
(
    SELECT
        -- R02: UPPER + TRIM + quitar sufijos corporativos comunes
        RTRIM(LTRIM(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                UPPER(RTRIM(LTRIM(@nombre))),
            ' INC.',''), ' CO.',''), ' LTD.',''), ', INC',''), ' LLC','')
        )) AS NombreNormalizado
);
GO


-- ============================================================
-- 1. PAISES
--    Fuente: stg_country_codes  (Code, Description)
--    Sin reglas de descarte propias; se hace UPSERT (MERGE)
-- ============================================================
PRINT '>>> [1/6] Cargando PAISES...';

MERGE INTO dbo.PAISES AS tgt
USING (
    SELECT DISTINCT
        RTRIM(LTRIM(Code))        AS CodigoPais,
        RTRIM(LTRIM(Description)) AS NombrePais
    FROM dw_staging_raw.dbo.stg_country_codes
    WHERE Code IS NOT NULL
      AND RTRIM(LTRIM(Code)) <> ''
) AS src
ON tgt.CodigoPais = src.CodigoPais
WHEN NOT MATCHED BY TARGET THEN
    INSERT (CodigoPais, NombrePais)
    VALUES (src.CodigoPais, src.NombrePais)
WHEN MATCHED AND tgt.NombrePais <> src.NombrePais THEN
    UPDATE SET tgt.NombrePais = src.NombrePais;

PRINT '    OK - filas afectadas: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO


-- ============================================================
-- 2. REGIONES_WAC
--    Fuente: stg_world_area_codes  (Code, Description)
-- ============================================================
PRINT '>>> [2/6] Cargando REGIONES_WAC...';

MERGE INTO dbo.REGIONES_WAC AS tgt
USING (
    SELECT DISTINCT
        CAST(RTRIM(LTRIM(Code)) AS SMALLINT) AS CodigoWAC,
        RTRIM(LTRIM(Description))            AS DescripcionWAC
    FROM dw_staging_raw.dbo.stg_world_area_codes
    WHERE Code IS NOT NULL
      AND ISNUMERIC(RTRIM(LTRIM(Code))) = 1
) AS src
ON tgt.CodigoWAC = src.CodigoWAC
WHEN NOT MATCHED BY TARGET THEN
    INSERT (CodigoWAC, DescripcionWAC)
    VALUES (src.CodigoWAC, src.DescripcionWAC)
WHEN MATCHED AND tgt.DescripcionWAC <> src.DescripcionWAC THEN
    UPDATE SET tgt.DescripcionWAC = src.DescripcionWAC;

PRINT '    OK - filas afectadas: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO


-- ============================================================
-- 3. AEROPUERTOS
--    Fuentes: stg_airports (Code=IATA, Description=Nombre+Ciudad)
--             stg_airport_ids (Code=AirportID numerico)
--             stg_airport_seq_ids (Code=SeqID periodo)
--             stg_city_market_ids (Code=CityMarketID)
--             stg_t100 (columnas de origen/destino para FK de pais/WAC)
--
--    R09: se usa el AirportSeqID real (de stg_airport_seq_ids)
--         que codifica el periodo; NO se mezcla con AirportID base.
--
--    Estrategia: construir conjunto canonico a partir de los
--    valores distintos de (OriginAirportSeqID, OriginAirportID,
--    OriginCityMarketID, Origin, OriginCityName, OriginCountry,
--    OriginWAC) UNION destino, aplicando UNION para eliminar
--    duplicados entre origen y destino.
-- ============================================================
PRINT '>>> [3/6] Cargando AEROPUERTOS...';

-- CTE que consolida origen y destino de stg_t100
;WITH stg_aeropuertos_raw AS (
    SELECT DISTINCT
        OriginAirportSeqID  AS AirportSeqID,   -- R09: SeqID real, no el base
        OriginAirportID     AS AirportID,
        OriginCityMarketID  AS CityMarketID,
        Origin              AS CodigoIATA,
        OriginCityName      AS Ciudad,
        OriginCountry       AS CodigoPais,
        OriginWAC           AS CodigoWAC
    FROM dw_staging_raw.dbo.stg_t100
    WHERE Origin IS NOT NULL
      AND OriginAirportSeqID IS NOT NULL
      -- R09: rechazar filas donde SeqID == AirportID base (confusion)
      AND OriginAirportSeqID <> OriginAirportID

    UNION

    SELECT DISTINCT
        DestAirportSeqID,
        DestAirportID,
        DestCityMarketID,
        Dest,
        DestCityName,
        DestCountry,
        DestWAC
    FROM dw_staging_raw.dbo.stg_t100
    WHERE Dest IS NOT NULL
      AND DestAirportSeqID IS NOT NULL
      -- R09: rechazar filas donde SeqID == AirportID base (confusion)
      AND DestAirportSeqID <> DestAirportID
),
-- Enrichment: nombre del aeropuerto desde lookup stg_airports
stg_aeropuertos_enrich AS (
    SELECT
        r.AirportSeqID,
        r.AirportID,
        r.CityMarketID,
        r.CodigoIATA,
        -- Nombre del aeropuerto: viene en Description del lookup
        -- formato tipico "Ciudad, Pais: Nombre" -> tomamos Description completo
        COALESCE(a.Description, r.Ciudad) AS NombreAeropuerto,
        r.Ciudad,
        r.CodigoPais,
        r.CodigoWAC
    FROM stg_aeropuertos_raw r
    LEFT JOIN dw_staging_raw.dbo.stg_airports a
           ON RTRIM(LTRIM(a.Code)) = RTRIM(LTRIM(r.CodigoIATA))
)
MERGE INTO dbo.AEROPUERTOS AS tgt
USING (
    SELECT
        e.AirportSeqID,
        e.AirportID,
        e.CityMarketID,
        RTRIM(LTRIM(e.CodigoIATA))      AS CodigoIATA,
        RTRIM(LTRIM(e.NombreAeropuerto)) AS NombreAeropuerto,
        RTRIM(LTRIM(e.Ciudad))          AS Ciudad,
        p.PaisID,
        w.WACID
    FROM stg_aeropuertos_enrich e
    LEFT JOIN dbo.PAISES p
           ON p.CodigoPais = RTRIM(LTRIM(e.CodigoPais))
    LEFT JOIN dbo.REGIONES_WAC w
           ON w.CodigoWAC = e.CodigoWAC
    WHERE e.CodigoIATA IS NOT NULL
      AND RTRIM(LTRIM(e.CodigoIATA)) <> ''
) AS src
ON tgt.AirportSeqID = src.AirportSeqID   -- clave natural unica (R09)
WHEN NOT MATCHED BY TARGET THEN
    INSERT (AirportSeqID, AirportID, CityMarketID,
            CodigoIATA, NombreAeropuerto, Ciudad, PaisID, WACID)
    VALUES (src.AirportSeqID, src.AirportID, src.CityMarketID,
            src.CodigoIATA, src.NombreAeropuerto, src.Ciudad,
            src.PaisID, src.WACID)
WHEN MATCHED THEN
    UPDATE SET
        tgt.AirportID      = src.AirportID,
        tgt.CityMarketID   = src.CityMarketID,
        tgt.NombreAeropuerto = src.NombreAeropuerto,
        tgt.Ciudad         = src.Ciudad,
        tgt.PaisID         = src.PaisID,
        tgt.WACID          = src.WACID;

PRINT '    OK - filas afectadas: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO


-- ============================================================
-- 4. GRUPOS_TRANSPORTISTA
--    Fuente: stg_carrier_group_new  (clasificacion vigente)
--    Fallback: stg_carrier_group    (clasificacion anterior)
-- ============================================================
PRINT '>>> [4/6] Cargando GRUPOS_TRANSPORTISTA...';

MERGE INTO dbo.GRUPOS_TRANSPORTISTA AS tgt
USING (
    -- Preferimos la clasificacion nueva; completamos con la vieja
    SELECT DISTINCT
        RTRIM(LTRIM(Code))        AS CodigoGrupo,
        RTRIM(LTRIM(Description)) AS NombreGrupo
    FROM dw_staging_raw.dbo.stg_carrier_group_new
    WHERE Code IS NOT NULL AND RTRIM(LTRIM(Code)) <> ''

    UNION

    SELECT DISTINCT
        RTRIM(LTRIM(Code)),
        RTRIM(LTRIM(Description))
    FROM dw_staging_raw.dbo.stg_carrier_group
    WHERE Code IS NOT NULL AND RTRIM(LTRIM(Code)) <> ''
) AS src
ON tgt.CodigoGrupo = src.CodigoGrupo
WHEN NOT MATCHED BY TARGET THEN
    INSERT (CodigoGrupo, NombreGrupo)
    VALUES (src.CodigoGrupo, src.NombreGrupo)
WHEN MATCHED AND tgt.NombreGrupo <> src.NombreGrupo THEN
    UPDATE SET tgt.NombreGrupo = src.NombreGrupo;

PRINT '    OK - filas afectadas: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO


-- ============================================================
-- 5. TRANSPORTISTAS  +  HISTORIAL_TRANSPORTISTA
--
--    Fuentes: stg_t100 (AirlineID, UniqueCarrier, UniqueCarrierName,
--                       CarrierGroup, CarrierGroupNew)
--             stg_unique_carriers (lookup oficial Code/Description)
--             stg_carrier_history (historial fusiones/cambios)
--
--    R01: AirlineID es la clave estable; se descarta el sufijo
--         numerico de UniqueCarrier (ej. "AA(1)" -> "AA").
--    R02: UniqueCarrierName normalizado via fn_NormalizarNombreCarrier.
--    R03: AirlineID = 0 o NULL -> registro especial "Desconocido".
--    R11: Activo = 1 si la fecha actual <= EndDate (o EndDate NULL
--         con carrier aun operando); Activo = 0 si cerro/fue absorbido.
-- ============================================================
PRINT '>>> [5/6] Cargando TRANSPORTISTAS y HISTORIAL...';

-- 5a. Registro especial para AirlineID invalido (R03)
IF NOT EXISTS (SELECT 1 FROM dbo.TRANSPORTISTAS WHERE AirlineID = 0)
BEGIN
    INSERT INTO dbo.TRANSPORTISTAS
        (AirlineID, UniqueCarrierCode, UniqueCarrierName,
         GrupoTransportistaID, Activo, FechaDesde, FechaHasta)
    VALUES
        (0, 'UNK', 'DESCONOCIDO', NULL, 0, NULL, NULL);
END;

-- 5b. Carriers reales: conjunto canonico desde stg_t100
;WITH carriers_raw AS (
    -- R01: quitar sufijo numerico entre parentesis del UniqueCarrier
    SELECT DISTINCT
        AirlineID,
        -- Elimina el patron "(N)" del codigo IATA
        RTRIM(LTRIM(
            CASE
                WHEN UniqueCarrier LIKE '%(%)%'
                THEN LEFT(UniqueCarrier, CHARINDEX('(', UniqueCarrier) - 1)
                ELSE UniqueCarrier
            END
        ))                    AS UniqueCarrierCode,
        UniqueCarrierName,
        CarrierGroupNew,      -- grupo nuevo preferido
        CarrierGroup          -- grupo viejo como fallback
    FROM dw_staging_raw.dbo.stg_t100
    -- R03: excluir AirlineID = 0 o NULL (ya insertado como Desconocido)
    WHERE AirlineID IS NOT NULL
      AND AirlineID <> 0
),
-- Por AirlineID puede haber varias filas (distintos meses/rutas);
-- tomamos la primera segun UniqueCarrierCode alfabetico
carriers_dedup AS (
    SELECT
        AirlineID,
        UniqueCarrierCode,
        UniqueCarrierName,
        CarrierGroupNew,
        CarrierGroup,
        ROW_NUMBER() OVER (
            PARTITION BY AirlineID
            ORDER BY UniqueCarrierCode
        ) AS rn
    FROM carriers_raw
),
-- R02: aplicar normalizacion de nombre
carriers_norm AS (
    SELECT
        cd.AirlineID,
        cd.UniqueCarrierCode,
        fn.NombreNormalizado                              AS UniqueCarrierName,
        COALESCE(cd.CarrierGroupNew, cd.CarrierGroup)    AS CodigoGrupo
    FROM carriers_dedup cd
    CROSS APPLY dbo.fn_NormalizarNombreCarrier(cd.UniqueCarrierName) fn
    WHERE cd.rn = 1
),
-- R11: calcular flag Activo cruzando con stg_carrier_history
--   Activo = 0 si hay registro en historial con EndDate < hoy
--   y no existe otro registro mas reciente para el mismo AirlineID
activo_calc AS (
    SELECT
        h.AirlineID,
        MAX(CASE
            WHEN h.EndDate IS NULL OR RTRIM(LTRIM(h.EndDate)) = ''
                THEN 1   -- sin fecha de cierre => asumimos activo
            WHEN TRY_CAST(RTRIM(LTRIM(h.EndDate)) AS DATE) >= CAST(GETDATE() AS DATE)
                THEN 1   -- fecha futura => activo
            ELSE 0
        END)              AS Activo,
        MIN(TRY_CAST(RTRIM(LTRIM(h.StartDate)) AS DATE)) AS FechaDesde,
        MAX(CASE
            WHEN h.EndDate IS NULL OR RTRIM(LTRIM(h.EndDate)) = '' THEN NULL
            ELSE TRY_CAST(RTRIM(LTRIM(h.EndDate)) AS DATE)
        END)              AS FechaHasta
    FROM dw_staging_raw.dbo.stg_carrier_history h
    WHERE h.AirlineID IS NOT NULL
      AND ISNUMERIC(RTRIM(LTRIM(h.AirlineID))) = 1
      AND CAST(RTRIM(LTRIM(h.AirlineID)) AS INT) <> 0
    GROUP BY h.AirlineID
)
MERGE INTO dbo.TRANSPORTISTAS AS tgt
USING (
    SELECT
        cn.AirlineID,
        cn.UniqueCarrierCode,
        cn.UniqueCarrierName,
        g.GrupoTransportistaID,
        COALESCE(ac.Activo, 1)     AS Activo,     -- R11
        ac.FechaDesde,
        ac.FechaHasta
    FROM carriers_norm cn
    LEFT JOIN dbo.GRUPOS_TRANSPORTISTA g
           ON g.CodigoGrupo = cn.CodigoGrupo
    LEFT JOIN activo_calc ac
           ON CAST(ac.AirlineID AS INT) = cn.AirlineID
) AS src
ON tgt.AirlineID = src.AirlineID                 -- R01: AirlineID como clave
WHEN NOT MATCHED BY TARGET THEN
    INSERT (AirlineID, UniqueCarrierCode, UniqueCarrierName,
            GrupoTransportistaID, Activo, FechaDesde, FechaHasta)
    VALUES (src.AirlineID, src.UniqueCarrierCode, src.UniqueCarrierName,
            src.GrupoTransportistaID, src.Activo, src.FechaDesde, src.FechaHasta)
WHEN MATCHED THEN
    UPDATE SET
        tgt.UniqueCarrierCode    = src.UniqueCarrierCode,
        tgt.UniqueCarrierName    = src.UniqueCarrierName,
        tgt.GrupoTransportistaID = src.GrupoTransportistaID,
        tgt.Activo               = src.Activo,
        tgt.FechaDesde           = src.FechaDesde,
        tgt.FechaHasta           = src.FechaHasta;

PRINT '    TRANSPORTISTAS OK - filas afectadas: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO

-- 5c. HISTORIAL_TRANSPORTISTA
--     Fuente: stg_carrier_history (AirlineID, UniqueCarrier, CarrierName,
--                                   StartDate, EndDate)
--     Cada fila del historial representa un cambio de codigo o nombre.
--     Se cruza con TRANSPORTISTAS para obtener TransportistaID (FK).
INSERT INTO dbo.HISTORIAL_TRANSPORTISTA
    (TransportistaID, CodigoAnterior, CodigoNuevo, FechaCambio)
SELECT
    t.TransportistaID,
    -- CodigoAnterior = codigo IATA en ese registro historico
    RTRIM(LTRIM(h.UniqueCarrier))                           AS CodigoAnterior,
    -- CodigoNuevo = codigo actual en TRANSPORTISTAS
    t.UniqueCarrierCode                                     AS CodigoNuevo,
    TRY_CAST(RTRIM(LTRIM(h.StartDate)) AS DATE)            AS FechaCambio
FROM dw_staging_raw.dbo.stg_carrier_history h
INNER JOIN dbo.TRANSPORTISTAS t
        ON t.AirlineID = CAST(RTRIM(LTRIM(h.AirlineID)) AS INT)
WHERE h.AirlineID IS NOT NULL
  AND ISNUMERIC(RTRIM(LTRIM(h.AirlineID))) = 1
  AND CAST(RTRIM(LTRIM(h.AirlineID)) AS INT) <> 0
  -- Excluir si el codigo historico ya es igual al actual (sin cambio real)
  AND RTRIM(LTRIM(h.UniqueCarrier)) <> t.UniqueCarrierCode
  -- Evitar duplicados si el script se re-ejecuta
  AND NOT EXISTS (
      SELECT 1 FROM dbo.HISTORIAL_TRANSPORTISTA hx
      WHERE hx.TransportistaID = t.TransportistaID
        AND hx.CodigoAnterior  = RTRIM(LTRIM(h.UniqueCarrier))
        AND hx.FechaCambio     = TRY_CAST(RTRIM(LTRIM(h.StartDate)) AS DATE)
  );

PRINT '    HISTORIAL_TRANSPORTISTA OK - filas afectadas: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO


-- ============================================================
-- 6. TIPOS_AERONAVE
--    Fuente: stg_aircraft_group + stg_aircraft_type + stg_aircraft_config
--            Cruzados con los valores distintos de stg_t100 para
--            cargar solo los tipos realmente usados.
-- ============================================================
PRINT '>>> Cargando TIPOS_AERONAVE...';

MERGE INTO dbo.TIPOS_AERONAVE AS tgt
USING (
    SELECT DISTINCT
        RTRIM(LTRIM(t.AircraftGroup))  AS CodigoGrupo,
        RTRIM(LTRIM(t.AircraftType))   AS CodigoTipo,
        RTRIM(LTRIM(t.AircraftConfig)) AS Configuracion,
        COALESCE(
            RTRIM(LTRIM(at2.Description)),
            RTRIM(LTRIM(t.AircraftType))
        )                              AS Descripcion
    FROM dw_staging_raw.dbo.stg_t100 t
    LEFT JOIN dw_staging_raw.dbo.stg_aircraft_type at2
           ON RTRIM(LTRIM(at2.Code)) = RTRIM(LTRIM(t.AircraftType))
    WHERE t.AircraftType IS NOT NULL
      AND RTRIM(LTRIM(t.AircraftType)) <> ''
) AS src
ON tgt.CodigoTipo = src.CodigoTipo
   AND tgt.CodigoGrupo = src.CodigoGrupo
   AND tgt.Configuracion = src.Configuracion
WHEN NOT MATCHED BY TARGET THEN
    INSERT (CodigoGrupo, CodigoTipo, Configuracion, Descripcion)
    VALUES (src.CodigoGrupo, src.CodigoTipo, src.Configuracion, src.Descripcion);

PRINT '    OK - filas afectadas: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO


-- ============================================================
-- 7. CLASES_SERVICIO
--    Fuente: stg_service_class  (Code, Description)
-- ============================================================
PRINT '>>> Cargando CLASES_SERVICIO...';

MERGE INTO dbo.CLASES_SERVICIO AS tgt
USING (
    SELECT DISTINCT
        RTRIM(LTRIM(Code))        AS CodigoClase,
        RTRIM(LTRIM(Description)) AS DescripcionClase
    FROM dw_staging_raw.dbo.stg_service_class
    WHERE Code IS NOT NULL AND RTRIM(LTRIM(Code)) <> ''
) AS src
ON tgt.CodigoClase = src.CodigoClase
WHEN NOT MATCHED BY TARGET THEN
    INSERT (CodigoClase, DescripcionClase)
    VALUES (src.CodigoClase, src.DescripcionClase);

PRINT '    OK - filas afectadas: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO


-- ============================================================
-- 8. GRUPOS_DISTANCIA
--    Fuente: stg_distance_group  (Code, Description)
-- ============================================================
PRINT '>>> Cargando GRUPOS_DISTANCIA...';

MERGE INTO dbo.GRUPOS_DISTANCIA AS tgt
USING (
    SELECT DISTINCT
        CAST(RTRIM(LTRIM(Code)) AS SMALLINT) AS CodigoGrupo,
        RTRIM(LTRIM(Description))            AS DescripcionGrupo
    FROM dw_staging_raw.dbo.stg_distance_group
    WHERE Code IS NOT NULL
      AND ISNUMERIC(RTRIM(LTRIM(Code))) = 1
) AS src
ON tgt.CodigoGrupo = src.CodigoGrupo
WHEN NOT MATCHED BY TARGET THEN
    INSERT (CodigoGrupo, DescripcionGrupo)
    VALUES (src.CodigoGrupo, src.DescripcionGrupo);

PRINT '    OK - filas afectadas: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO


-- ============================================================
-- 9. SEGMENTOS_VUELO  (tabla de hechos - ultima en cargarse)
--
--    R04: DepPerformed > DepScheduled => DESCARTE
--    R05: Passengers > Seats OR (Seats=0 AND Passengers>0) => DESCARTE
--    R06: Distance <= 0 => DESCARTE
--    R07: AirTime <= 0 OR AirTime > 1200 => DESCARTE
--    R08: Freight > Payload OR Freight < 0 => DESCARTE
--    R09: Join a AEROPUERTOS usando AirportSeqID (no AirportID base)
--    R10: ROW_NUMBER() para deduplicar antes de insertar
--
--    Se usa INSERT...SELECT (no MERGE) porque SEGMENTOS_VUELO
--    es append-only: cada carga representa un periodo nuevo.
--    Si se requiere idempotencia, agregar DELETE previo por Year/Month.
-- ============================================================
PRINT '>>> [6/6] Cargando SEGMENTOS_VUELO (tabla de hechos)...';

-- Borrar el periodo que se va a recargar para garantizar idempotencia
-- (ajustar Year segun el periodo real que se procesa)
DECLARE @AnioRecarga INT = 2023;

DELETE FROM dbo.SEGMENTOS_VUELO
WHERE Anio = @AnioRecarga;

PRINT '    Registros del anio ' + CAST(@AnioRecarga AS VARCHAR) +
      ' eliminados para recarga: ' + CAST(@@ROWCOUNT AS VARCHAR);

-- CTE principal con todas las reglas de calidad
;WITH

-- R10: Deduplicacion - asignamos numero de fila dentro de cada
--      particion natural del segmento de vuelo
base_dedup AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY
                Year,
                Month,
                UniqueCarrier,
                Origin,
                Dest,
                Class,
                AircraftType
            ORDER BY (SELECT NULL)   -- orden arbitrario entre duplicados
        ) AS rn
    FROM dw_staging_raw.dbo.stg_t100
),

-- Aplicar todas las reglas de descarte sobre el conjunto deduplicado
base_limpia AS (
    SELECT *
    FROM base_dedup
    WHERE rn = 1                                      -- R10: solo el primer registro

    -- R03: Excluir AirlineID invalido
    AND AirlineID IS NOT NULL
    AND AirlineID <> 0

    -- R04: Vuelos realizados no pueden superar los programados
    AND NOT (DepPerformed > DepScheduled)

    -- R05: Pasajeros no pueden superar asientos disponibles
    --      Tambien descarta Seats=0 con pasajeros (error de origen)
    AND NOT (Passengers > Seats)
    AND NOT (Seats = 0 AND Passengers > 0)

    -- R06: Distancia debe ser positiva
    AND Distance > 0

    -- R07: Tiempo de vuelo en rango valido (1 min - 20 hs)
    AND AirTime > 0
    AND AirTime <= 1200

    -- R08: Carga no puede superar payload total ni ser negativa
    AND NOT (Freight > Payload)
    AND NOT (Freight < 0)

    -- Filtros adicionales de integridad basica
    AND OriginAirportSeqID IS NOT NULL
    AND DestAirportSeqID   IS NOT NULL
    -- R09: rechazar filas con SeqID = AirportID (confusion de campos)
    AND OriginAirportSeqID <> OriginAirportID
    AND DestAirportSeqID   <> DestAirportID
)

-- INSERT final en SEGMENTOS_VUELO resolviendo todas las FK
INSERT INTO dbo.SEGMENTOS_VUELO (
    Anio, Trimestre, Mes,
    TransportistaID,
    OrigenAeropuertoID, DestinoAeropuertoID,
    TipoAeronaveID,
    ClaseServicioID,
    GrupoDistanciaID,
    VuelosProgramados, VuelosRealizados,
    AsientosDisponibles, PasajerosTransportados,
    CargaLibras, CorreoLibras,
    DistanciaMillas, TiempoVuelo
)
SELECT
    b.Year                                  AS Anio,
    b.Quarter                               AS Trimestre,
    b.Month                                 AS Mes,

    -- R01 / R03: FK a TRANSPORTISTAS usando AirlineID
    --            Si no existe, mapea al registro "Desconocido" (AirlineID=0)
    COALESCE(tr.TransportistaID,
             (SELECT TransportistaID FROM dbo.TRANSPORTISTAS WHERE AirlineID = 0))
                                            AS TransportistaID,

    -- R09: FK a AEROPUERTOS usando AirportSeqID (no AirportID base)
    ao.AeropuertoID                         AS OrigenAeropuertoID,
    ad.AeropuertoID                         AS DestinoAeropuertoID,

    -- FK a TIPOS_AERONAVE (por grupo + tipo + config)
    ta.TipoAeronaveID,

    -- FK a CLASES_SERVICIO
    cs.ClaseServicioID,

    -- FK a GRUPOS_DISTANCIA
    gd.GrupoDistanciaID,

    -- Metricas operacionales
    b.DepScheduled                          AS VuelosProgramados,
    b.DepPerformed                          AS VuelosRealizados,
    b.Seats                                 AS AsientosDisponibles,
    b.Passengers                            AS PasajerosTransportados,
    b.Freight                               AS CargaLibras,
    b.Mail                                  AS CorreoLibras,
    b.Distance                              AS DistanciaMillas,
    b.AirTime                               AS TiempoVuelo

FROM base_limpia b

-- Transportista (R01: join por AirlineID)
LEFT JOIN dbo.TRANSPORTISTAS tr
       ON tr.AirlineID = b.AirlineID

-- Aeropuerto origen (R09: join por AirportSeqID)
LEFT JOIN dbo.AEROPUERTOS ao
       ON ao.AirportSeqID = b.OriginAirportSeqID

-- Aeropuerto destino (R09: join por AirportSeqID)
LEFT JOIN dbo.AEROPUERTOS ad
       ON ad.AirportSeqID = b.DestAirportSeqID

-- Tipo de aeronave
LEFT JOIN dbo.TIPOS_AERONAVE ta
       ON ta.CodigoGrupo    = RTRIM(LTRIM(b.AircraftGroup))
      AND ta.CodigoTipo     = RTRIM(LTRIM(b.AircraftType))
      AND ta.Configuracion  = RTRIM(LTRIM(b.AircraftConfig))

-- Clase de servicio
LEFT JOIN dbo.CLASES_SERVICIO cs
       ON cs.CodigoClase = RTRIM(LTRIM(b.Class))

-- Grupo de distancia
LEFT JOIN dbo.GRUPOS_DISTANCIA gd
       ON gd.CodigoGrupo = b.DistanceGroup

-- Descartar filas donde aeropuertos no resolvieron FK
-- (no existen en el modelo destino por errores de datos en origen)
WHERE ao.AeropuertoID IS NOT NULL
  AND ad.AeropuertoID IS NOT NULL;

PRINT '    SEGMENTOS_VUELO OK - filas insertadas: ' + CAST(@@ROWCOUNT AS VARCHAR);
GO


-- ============================================================
-- 10. VERIFICACION FINAL DE CONTEOS
-- ============================================================
PRINT '>>> Verificacion de conteos en modelo OLTP:';

SELECT 'PAISES'                  AS Tabla, COUNT(*) AS Filas FROM dbo.PAISES
UNION ALL
SELECT 'REGIONES_WAC',                     COUNT(*)          FROM dbo.REGIONES_WAC
UNION ALL
SELECT 'AEROPUERTOS',                      COUNT(*)          FROM dbo.AEROPUERTOS
UNION ALL
SELECT 'GRUPOS_TRANSPORTISTA',             COUNT(*)          FROM dbo.GRUPOS_TRANSPORTISTA
UNION ALL
SELECT 'TRANSPORTISTAS',                   COUNT(*)          FROM dbo.TRANSPORTISTAS
UNION ALL
SELECT 'HISTORIAL_TRANSPORTISTA',          COUNT(*)          FROM dbo.HISTORIAL_TRANSPORTISTA
UNION ALL
SELECT 'TIPOS_AERONAVE',                   COUNT(*)          FROM dbo.TIPOS_AERONAVE
UNION ALL
SELECT 'CLASES_SERVICIO',                  COUNT(*)          FROM dbo.CLASES_SERVICIO
UNION ALL
SELECT 'GRUPOS_DISTANCIA',                 COUNT(*)          FROM dbo.GRUPOS_DISTANCIA
UNION ALL
SELECT 'SEGMENTOS_VUELO',                  COUNT(*)          FROM dbo.SEGMENTOS_VUELO
ORDER BY Tabla;
GO

PRINT '>>> ETL Transform + Load completado exitosamente.';
GO
