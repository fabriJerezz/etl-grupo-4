Documentacion - load.py
=======================

Proposito
---------
Este script ejecuta la fase de Carga (Load) del pipeline ETL. Toma el DataFrame limpio y transformado, y puebla el modelo del Data Warehouse destino (`dw_trafico_aereo`).
Está diseñado para realizar una **carga inicial completa** (Full Load), por lo que elimina los datos existentes en el DW y reinicia los contadores de las claves subrogadas antes de cada ejecución, garantizando la idempotencia del proceso.

Requisitos
----------
- Python 3.x
- Paquetes: pandas, numpy, sqlalchemy, python-dotenv
- Driver ODBC para SQL Server (ODBC Driver 17 o 18)
- Modulo local `transform.py` accesible en el entorno.

Instalacion rapida de dependencias:
		pip install -r requirements.txt

Configuracion
-------------
1) DB_CONNECTION_STRING en archivo .env
	 El script asume la existencia de la variable de entorno configurada para Staging.
	 Internamente, reemplaza el nombre de la base de datos `dw_staging_raw` por `dw_trafico_aereo` para conectarse al Data Warehouse.
	 
	 Ejemplo en .env:
	 DB_CONNECTION_STRING=mssql+pyodbc://@localhost/dw_staging_raw?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes

Entradas
--------
- Un `pandas.DataFrame` validado y limpio, proveniente de la ejecución de `transform.ejecutar_pipeline_transformacion()`.

Salidas
-------
- Tablas dimensionales pobladas en el Data Warehouse: `Tiempo`, `Aeropuerto`, `Transportista`.
- Tabla de hechos poblada en el Data Warehouse: `Vuelo`.
- Mensajes en consola detallando el tiempo de ejecución y confirmando la inserción.

Flujo general
-------------
1) Limpieza (Truncate/Delete): Borra los registros de la tabla de hechos y luego de las dimensiones. Reinicia los contadores IDENTITY a 0.
2) Carga de Dimensiones:
   - Tiempo: Extrae valores únicos de Mes, Año y Estación. Mapea numéricamente las estaciones a etiquetas (Invierno, Primavera, etc.).
   - Aeropuerto: Unifica orígenes y destinos, elimina duplicados forzando unicidad por código IATA e inyecta campos de control SCD (Fecha_Inicio, Fila_Activa).
   - Transportista: Extrae aerolíneas, elimina duplicados por nombre e inyecta campos de control SCD.
3) Mapeo de Surrogate Keys: Consulta las tablas dimensionales recién cargadas en SQL Server para recuperar los IDs autogenerados (Keys) y cruza esta información con el DataFrame principal (Left Join).
4) Carga de Tabla de Hechos: Filtra exclusivamente las métricas y las claves foráneas, renombra las columnas para coincidir con el esquema físico y realiza una inserción masiva delegando el Identity a SQL Server.

Funciones principales
---------------------
- carga_inicial_dw(df)
	Función orquestadora que recibe el DataFrame transformado y ejecuta secuencialmente la limpieza, la partición de dimensiones, el mapeo de llaves subrogadas y la inserción de la tabla de hechos en la base de datos `dw_trafico_aereo`.

Mapeo del Modelo
----------------------------
DataFrame Transformado -> Tabla Data Warehouse

**Dimensiones (Padres):**
- ['Year', 'Month', 'Estacion']                   -> Tiempo
- ['Origin', 'OriginCityName', 'OriginCountry']   -> Aeropuerto (como origen)
- ['Dest', 'DestCityName', 'DestCountry']         -> Aeropuerto (como destino)
- ['UniqueCarrierName', 'CarrierGroupNew', ...]   -> Transportista

**Tabla de Hechos (Hija):**
- Métricas consolidadas + Surrogate Keys mapeadas -> Vuelo

Ejecucion
---------
Como parte del pipeline principal (Recomendado):
		python main.py

De manera aislada (solo para pruebas):
		python load.py

Notas de uso
------------
- Idempotencia: El paso 1 utiliza `DELETE FROM` y `DBCC CHECKIDENT` para asegurar que las pruebas iterativas no acumulen basura ni desfasen los IDs. Al existir restricciones de Foreign Keys, se borra primero la tabla hija (`Vuelo`) y luego las dimensiones.
- Atributos SCD Tipo 2: Se añaden columnas por defecto (`Fecha_Inicio`, `Fila_Activa` = 1) preparando el modelo para futuras cargas incrementales e historización.
- Rendimiento: Al igual que en la extracción, la carga utiliza `fast_executemany=True` en la configuración del engine de SQLAlchemy para optimizar las inserciones masivas en la base de datos.

Errores comunes
--------------
- Infracción de Claves Foráneas (FK): Si la base de datos impide el DELETE de una dimensión, verificar que la tabla `Vuelo` haya sido vaciada primero en el código.
- Problemas de Cruce (Mapeo de Keys): Si la tabla `Vuelo` se carga con llaves nulas, significa que el `LEFT JOIN` en el Paso 3 falló debido a problemas de espacios invisibles o inconsistencias de mayúsculas/minúsculas entre el DataFrame y la base de datos.
- Base de Datos Inexistente: Asegurar que el catálogo `dw_trafico_aereo` haya sido creado previamente en SQL Server antes de correr el script.
