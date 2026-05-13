Documentacion - transform.py
============================

Proposito
---------
Este script ejecuta la fase de Transformación (Transform) del pipeline ETL. Actúa como el motor de calidad de datos, tomando la información cruda de la base de Staging (`stg_t100`), aplicando una batería de 18 reglas de limpieza, imputación y validación, y calculando las métricas derivadas necesarias para el Data Warehouse.
Devuelve un DataFrame validado en memoria listo para la fase de Carga, y opcionalmente persiste una copia física en Staging para fines de auditoría.

Requisitos
----------
- Python 3.x
- Paquetes: pandas, numpy, sqlalchemy, python-dotenv, re, time, os
- Driver ODBC para SQL Server (ODBC Driver 17 o 18)

Instalacion rapida de dependencias:
		pip install -r requirements.txt

Configuracion
-------------
1) DB_CONNECTION_STRING en archivo .env
	 Cadena de conexión a la base de datos de Staging (`dw_staging_raw`) usando SQLAlchemy.
	 
	 Ejemplo:
	 DB_CONNECTION_STRING=mssql+pyodbc://@localhost/dw_staging_raw?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes

Entradas
--------
- Un `pandas.DataFrame` con los datos crudos extraídos mediante la consulta `SELECT * FROM stg_t100`.
- El objeto `engine` de SQLAlchemy para aquellas reglas que requieren consultar tablas de *lookup* (catálogos) directamente en la base de datos.

Salidas
-------
- Un `pandas.DataFrame` en memoria con los datos completamente limpios, transformados y tipados.
- **Tabla de Auditoría:** Si el script se ejecuta de forma aislada, guarda el resultado físico en la tabla `stg_t100_transformed` dentro de la base de Staging.

Flujo general
-------------
1) Lectura de datos desde `stg_t100`.
2) Orquestación de limpieza: Se aplican secuencialmente las reglas de calidad (priorizando primero aquellas reglas destructivas que eliminan registros nulos o inválidos, seguidas de las reglas de imputación y normalización).
3) Cálculo de métricas derivadas (Ocupación, Demoras y Estación temporal).
4) Retorno del DataFrame procesado para el módulo `load.py`.

Reglas de Calidad de Datos (R01 a R18)
--------------------------------------
El script aplica un catálogo estructurado de validaciones. *(Nota: La implementación técnica, el impacto en el negocio y la lógica detallada de cada regla se encuentran documentados en el PDF explicativo entregado adjunto al proyecto).*

- R01: Eliminación de sufijos en identificadores.
- R02: Normalización de nombres de aerolíneas.
- R03: Estandarización de `AirlineID` faltantes.
- R04: Imputación de nulos en campos obligatorios.
- R05: Validación de vuelos realizados (`DepPerformed`).
- R06: Validación cruzada de pasajeros y asientos.
- R07: Eliminación de registros con distancia cero o negativa.
- R08: Coherencia de tiempos de vuelo.
- R09: Tratamiento de códigos IATA reutilizados.
- R10: Eliminación de registros duplicados absolutos.
- R11: Inferencia histórica de secuencias de aeropuertos (`AirportSeqID`).
- R12: Inferencia y corrección de mercados geográficos (`CityMarketID`).
- R13: Consolidación de métricas de carga.
- R14: Corrección de coherencia de capacidad.
- R15: Imputación de métricas con valores negativos.
- R16: Depuración de aerolíneas expiradas.
- R17: Unificación de identificadores de Carrier.
- R18: Inferencia de región del transportista.

Cálculo de Métricas Derivadas
-----------------------------
Al finalizar el bloque de reglas, el script prepara los datos para el esquema multidimensional generando las siguientes métricas:
- **OcupPasajeros:** Ratio entre pasajeros y asientos disponibles (`Passengers / Seats`). Retorna 0 si los asientos son 0.
- **OcupCarga:** Ratio entre carga transportada y capacidad máxima de carga (`Freight / Payload`). Retorna 0 si la capacidad es 0.
- **DemoraPista:** Tiempo transcurrido en tierra (`RampTime - AirTime`).
- **Estacion:** Segmentación climática calculada a partir del mes del vuelo (1: Invierno, 2: Primavera, 3: Verano, 4: Otoño), requerida para la Dimensión Tiempo.

Ejecucion
---------
Como parte del pipeline principal (Recomendado):
		python main.py

De manera aislada (Para pruebas y generación de tabla de auditoría `stg_t100_transformed`):
		python transform.py

Notas de uso
------------
- Rendimiento de Auditoría: Al ejecutar el script de forma aislada, la carga de la tabla de auditoría utiliza `chunksize=10000` y `fast_executemany=True` para manejar eficientemente el volumen en memoria sin saturar el motor SQL.
- Tipado Dinámico: El script se asegura de castear internamente a `float` o `int` (vía `numpy`) antes de realizar operaciones matemáticas (ej. Ocupación), mitigando errores de tipo derivados de la lectura del CSV crudo.