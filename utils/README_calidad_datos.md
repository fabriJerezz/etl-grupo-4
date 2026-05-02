# Dataset T100 – Transportistas 2023
## Catálogo de Problemas de Calidad de Datos

Generado a partir de la fuente: https://www.transtats.bts.gov/  
Dataset base: T100 Segment – All Carriers, año 2023

---

## Archivos

| Archivo | Filas | Descripción |
|---------|-------|-------------|
| `t100_carriers_2023.csv` | 98.622 | Segmentos de vuelo con problemas inyectados |
| `l_unique_carriers.csv` | 42 | Lookup oficial de códigos de carrier |
| `l_carrier_history.csv` | 38 | Historial de cambios de código/nombre |
| `l_carrier_group.csv` | 12 | Grupos de transportistas |
| `l_carrier_group_new.csv` | 8 | Nueva clasificación de grupos |

---

## Problemas de Calidad en `t100_carriers_2023.csv`

| # | Problema | Campo afectado | Cant. registros | Descripción |
|---|----------|---------------|-----------------|-------------|
| 1 | **Sufijo numérico en código** | UniqueCarrier | ~54 | Mismo carrier con código `AA(1)` vs `AA`. Documentado en T100: cuando un código IATA fue reutilizado por más de un carrier, DOT agrega sufijo numérico al original. Para análisis cross-year se debe usar `AirlineID`, no `UniqueCarrier`. |
| 2 | **Variantes de nombre** | UniqueCarrierName | ~1.743 | Mismo carrier con nombres distintos: `"American Airlines Inc."` / `"AMERICAN AIRLINES INC."` / `"Amer. Airlines Inc."` / `"American Airlines, Inc."`. Dificulta agrupamiento por nombre. |
| 3 | **AirlineID = 0** | AirlineID | ~350 | Registros sin AirlineID asignado. Común en archivos históricos pre-2000 y en carriers extintos. Impide join con tablas de dimensión. |
| 4 | **Campos obligatorios vacíos** | CarrierName, AircraftType, OriginCity, DestCity, Class, OriginState | ~700 | Strings vacíos en campos que deberían tener valor. |
| 5 | **DepPerformed > DepScheduled** | DepScheduled, DepPerformed | ~480 | Vuelos realizados mayor a programados. Físicamente imposible; indica error de carga o reporte de operaciones de otro período. |
| 6 | **Pasajeros > Asientos disponibles** | Passengers, Seats | ~560 | Violación de restricción de negocio. Puede indicar reporte de pax de vuelos extra no contabilizados en Seats, o error de transcripción. |
| 7 | **Distance = 0 o negativa** | Distance | ~300 | Distancia entre aeropuertos nula o negativa. Impide cálculo de métricas de eficiencia. |
| 8 | **AirTime / RampTime imposibles** | AirTime, RampTime | ~380 | Valores negativos, cero, o >9000 minutos. |
| 9 | **Código IATA reutilizado** | UniqueCarrier + AirlineID | ~580 | Mismo UniqueCarrier (`CO`, `US`, `FL`, `VX`) con AirlineID de otro carrier post-fusión. Documentado: después de una fusión, el código del carrier absorbido puede quedar temporalmente asociado al AirlineID del absorbente. |
| 10 | **Filas duplicadas** | Todas | ~900 | Registros exactamente iguales. Ocurre cuando se carga dos veces el mismo archivo mensual del BTS sin control de duplicados. |
| 11 | **AirportSeqID = AirportID** | OriginAirportSeqID | ~500 | SeqID confundido con AirportID base. El SeqID incorpora el período de tiempo; sin él no se puede rastrear cambios de atributos del aeropuerto. |
| 12 | **CityMarketID = AirportID** | OriginCityMarketID | ~420 | ID de aeropuerto individual cargado en el campo de city market. Impide consolidar aeropuertos del mismo mercado (EWR/JFK/LGA → NY). |
| 13 | **Freight > Payload** | Freight, Mail, Payload | ~340 | Carga de mercancías mayor al payload total disponible. Imposible físicamente. |
| 14 | **Seats = 0 con Passengers > 0** | Seats, Passengers | ~220 | Asientos = 0 pero se reportan pasajeros. Indica que Seats no fue informado o fue truncado. |
| 15 | **Freight negativo** | Freight | ~180 | Valores negativos en campo de carga. |
| 16 | **Carriers inactivos con datos en 2023** | UniqueCarrier | ~346 | Registros de carriers fusionados o cerrados (CO, US, FL, VX, CP) que aparecen con datos en el año analizado. |

---

## Problemas de Calidad en tablas Lookup

### `l_unique_carriers.csv`
- **Códigos duplicados** con descripciones distintas: `AA` aparece como "American Airlines Inc." y "American Airlines"
- **Descripciones vacías** para carriers regionales (OO, 9E, C5)
- **Carriers inactivos sin baja**: CO, US, FL, VX siguen en el lookup sin indicador de vigencia
- **Código con sufijo histórico**: `AA(1)`, `CO(1)`, `US(1)`, `PA(1)` con mismos datos que la versión sin sufijo
- **Inconsistencias de formato**: ` AA` (espacio líder), `aa` (minúsculas), `BW ` (espacio final)

### `l_carrier_history.csv`
- **EndDate vacío para carriers inactivos**: Compass (CP) cesó en 2020 pero EndDate está en blanco
- **Código reutilizado post-fusión**: `CO` y `US` aparecen con dos AirlineID distintos en períodos solapados
- **Overlap de fechas**: AirTran (FL) y Southwest aparecen ambos con el código FL en el período de integración 2011-2014
- **AirlineID = 0** para carriers extintos pre-base (EA, PA)
- **Fechas invertidas**: Peninsula Airways con StartDate > EndDate
- **AirlineID de un carrier asignado a otro**: AirlineID 19386 (AA) aparece en registro de DL
- **EndDate en el futuro**: Compass con EndDate 2099-12-31
- **AirlineID inexistente**: 99999 para Allegiant

### `l_carrier_group.csv` y `l_carrier_group_new.csv`
- **Doble esquema**: grupos codificados numéricamente (1,2,3,4) en archivos nuevos y como texto (Major, National) en archivos viejos, sin tabla de equivalencia
- **Entradas duplicadas** del mismo código con descripción distinta
- **Código vacío** y **descripción vacía**
- **Sin mapeo explícito** entre carrier y grupo nuevo (`l_carrier_group_new`)

---

## Reglas de negocio para limpieza ETL

```sql
-- 1. Usar AirlineID como clave estable (no UniqueCarrier)
-- 2. Normalizar UniqueCarrierName: UPPER + TRIM + quitar sufijos (Inc., Co., Ltd.)
-- 3. Excluir AirlineID = 0 o manejar como "desconocido"
-- 4. Descartar: DepPerformed > DepScheduled
-- 5. Descartar: Passengers > Seats (salvo Seats = 0 → flag para revisión)
-- 6. Descartar: Distance <= 0
-- 7. Descartar: AirTime <= 0 OR AirTime > 1200 (20 hs máx vuelo doméstico)
-- 8. Corregir: AirportSeqID ≠ AirportID (buscar en lookup por período)
-- 9. Corregir: CityMarketID usando tabla L_CITY_MARKET_ID
-- 10. Descartar: Freight > Payload OR Freight < 0
-- 11. Deduplicar por (Year, Month, UniqueCarrier, Origin, Dest, Class, AircraftType)
-- 12. Marcar carriers inactivos con flag IsActive = 0 cruzando con l_carrier_history
```
