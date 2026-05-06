-- ============================================================
-- SCRIPT DE VERIFICACION POST-CARGA
-- Ejecutar DESPUES de transform_load.sql para validar resultados
-- ============================================================

USE dw_oltp;
GO

SET NOCOUNT ON;
PRINT '========================================';
PRINT ' VERIFICACION POST-CARGA - dw_oltp';
PRINT ' Fecha: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '========================================';

-- ============================================================
-- 1. CONTEO DE FILAS POR TABLA
-- ============================================================
PRINT '';
PRINT '--- 1. CONTEO DE FILAS ---';

SELECT
    t.name         AS Tabla,
    p.rows         AS TotalFilas
FROM sys.tables t
JOIN sys.partitions p
  ON p.object_id = t.object_id AND p.index_id IN (0,1)
WHERE t.schema_id = SCHEMA_ID('dbo')
ORDER BY
    CASE t.name
        WHEN 'PAISES'                  THEN 1
        WHEN 'REGIONES_WAC'            THEN 2
        WHEN 'AEROPUERTOS'             THEN 3
        WHEN 'GRUPOS_TRANSPORTISTA'    THEN 4
        WHEN 'TRANSPORTISTAS'          THEN 5
        WHEN 'HISTORIAL_TRANSPORTISTA' THEN 6
        WHEN 'TIPOS_AERONAVE'          THEN 7
        WHEN 'CLASES_SERVICIO'         THEN 8
        WHEN 'GRUPOS_DISTANCIA'        THEN 9
        WHEN 'SEGMENTOS_VUELO'         THEN 10
        ELSE 99
    END;

-- ============================================================
-- 2. INTEGRIDAD REFERENCIAL
--    Detecta FKs huerfanas (no deberia haber ninguna)
-- ============================================================
PRINT '';
PRINT '--- 2. CHEQUEO DE INTEGRIDAD REFERENCIAL ---';

-- Segmentos sin transportista valido
SELECT 'Segmentos sin Transportista' AS Problema, COUNT(*) AS Cantidad
FROM dbo.SEGMENTOS_VUELO sv
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.TRANSPORTISTAS t WHERE t.TransportistaID = sv.TransportistaID
)
UNION ALL
-- Segmentos sin aeropuerto de origen
SELECT 'Segmentos sin AeropuertoOrigen', COUNT(*)
FROM dbo.SEGMENTOS_VUELO sv
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.AEROPUERTOS a WHERE a.AeropuertoID = sv.OrigenAeropuertoID
)
UNION ALL
-- Segmentos sin aeropuerto de destino
SELECT 'Segmentos sin AeropuertoDestino', COUNT(*)
FROM dbo.SEGMENTOS_VUELO sv
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.AEROPUERTOS a WHERE a.AeropuertoID = sv.DestinoAeropuertoID
)
UNION ALL
-- Aeropuertos sin pais
SELECT 'Aeropuertos sin Pais', COUNT(*)
FROM dbo.AEROPUERTOS
WHERE PaisID IS NULL
UNION ALL
-- Transportistas sin grupo
SELECT 'Transportistas sin GrupoTransportista', COUNT(*)
FROM dbo.TRANSPORTISTAS
WHERE GrupoTransportistaID IS NULL AND AirlineID <> 0;

-- ============================================================
-- 3. REGLAS DE CALIDAD - VERIFICACION EN DESTINO
--    Ninguna de estas consultas deberia devolver filas.
-- ============================================================
PRINT '';
PRINT '--- 3. VIOLACIONES DE REGLAS DE NEGOCIO EN DESTINO ---';

SELECT 'R04 - VuelosRealizados > VuelosProgramados' AS Regla, COUNT(*) AS Violaciones
FROM dbo.SEGMENTOS_VUELO WHERE VuelosRealizados > VuelosProgramados
UNION ALL
SELECT 'R05 - Pasajeros > Asientos', COUNT(*)
FROM dbo.SEGMENTOS_VUELO WHERE PasajerosTransportados > AsientosDisponibles
UNION ALL
SELECT 'R06 - Distancia <= 0', COUNT(*)
FROM dbo.SEGMENTOS_VUELO WHERE DistanciaMillas <= 0
UNION ALL
SELECT 'R07 - TiempoVuelo fuera de rango', COUNT(*)
FROM dbo.SEGMENTOS_VUELO WHERE TiempoVuelo <= 0 OR TiempoVuelo > 1200
UNION ALL
SELECT 'R08 - CargaLibras negativa', COUNT(*)
FROM dbo.SEGMENTOS_VUELO WHERE CargaLibras < 0;

-- ============================================================
-- 4. METRICAS DE NEGOCIO DE SANIDAD
-- ============================================================
PRINT '';
PRINT '--- 4. METRICAS GENERALES (sanity check) ---';

SELECT
    COUNT(*)                                    AS TotalSegmentos,
    COUNT(DISTINCT TransportistaID)             AS TransportistasActivos,
    COUNT(DISTINCT OrigenAeropuertoID)          AS AeropuertosOrigen,
    COUNT(DISTINCT DestinoAeropuertoID)         AS AeropuertosDestino,
    SUM(VuelosRealizados)                       AS TotalVuelosRealizados,
    SUM(PasajerosTransportados)                 AS TotalPasajeros,
    CAST(AVG(CAST(DistanciaMillas AS FLOAT))
         AS DECIMAL(10,2))                      AS DistanciaPromedio_mi,
    CAST(AVG(CAST(TiempoVuelo AS FLOAT))
         AS DECIMAL(10,2))                      AS TiempoVueloPromedio_min
FROM dbo.SEGMENTOS_VUELO;

-- ============================================================
-- 5. TOP 10 CARRIERS POR PASAJEROS
-- ============================================================
PRINT '';
PRINT '--- 5. TOP 10 CARRIERS POR PASAJEROS ---';

SELECT TOP 10
    t.UniqueCarrierCode,
    t.UniqueCarrierName,
    SUM(sv.PasajerosTransportados)  AS TotalPasajeros,
    SUM(sv.VuelosRealizados)        AS TotalVuelos,
    CAST(
        SUM(sv.PasajerosTransportados) * 1.0 /
        NULLIF(SUM(sv.AsientosDisponibles), 0) * 100
    AS DECIMAL(5,2))                AS FactorOcupacion_pct
FROM dbo.SEGMENTOS_VUELO sv
JOIN dbo.TRANSPORTISTAS t ON t.TransportistaID = sv.TransportistaID
GROUP BY t.UniqueCarrierCode, t.UniqueCarrierName
ORDER BY TotalPasajeros DESC;

-- ============================================================
-- 6. DISTRIBUCION POR MES (verificar cobertura temporal)
-- ============================================================
PRINT '';
PRINT '--- 6. DISTRIBUCION POR MES ---';

SELECT
    Anio,
    Mes,
    COUNT(*)                        AS Segmentos,
    SUM(VuelosRealizados)           AS Vuelos,
    SUM(PasajerosTransportados)     AS Pasajeros
FROM dbo.SEGMENTOS_VUELO
GROUP BY Anio, Mes
ORDER BY Anio, Mes;

-- ============================================================
-- 7. CARRIERS INACTIVOS CON SEGMENTOS (alerta)
-- ============================================================
PRINT '';
PRINT '--- 7. CARRIERS INACTIVOS CON DATOS (revisar) ---';

SELECT
    t.AirlineID,
    t.UniqueCarrierCode,
    t.UniqueCarrierName,
    t.FechaHasta,
    COUNT(sv.SegmentoID) AS Segmentos
FROM dbo.TRANSPORTISTAS t
JOIN dbo.SEGMENTOS_VUELO sv ON sv.TransportistaID = t.TransportistaID
WHERE t.Activo = 0
GROUP BY t.AirlineID, t.UniqueCarrierCode, t.UniqueCarrierName, t.FechaHasta
ORDER BY Segmentos DESC;

PRINT '';
PRINT '========================================';
PRINT ' Verificacion completada.';
PRINT ' Revisar que la columna Violaciones = 0';
PRINT ' en la seccion 3 para aprobar la carga.';
PRINT '========================================';
GO
