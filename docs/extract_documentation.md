Documentacion - extract.py
==========================

Proposito
---------
Este script carga archivos CSV fuente en tablas de staging de SQL Server.
Primero carga los lookups (tablas pequeñas) y luego el archivo principal T100.
Antes de cada carga, trunca la tabla destino para mantener idempotencia.

Requisitos
----------
- Python 3.x
- Paquetes: pandas, sqlalchemy, pyodbc, python-dotenv
- Driver ODBC para SQL Server (ODBC Driver 17 o 18)

Instalacion rapida de dependencias:
		pip install -r requirements.txt

Configuracion
-------------
1) DB_CONNECTION_STRING en archivo .env
	 Crear un archivo .env en la raiz del proyecto (o copiar .env.example).
	 Cadena de conexion a SQL Server usando SQLAlchemy + pyodbc.
	 Ejemplo (autenticacion Windows):
	 DB_CONNECTION_STRING=mssql+pyodbc://@localhost/dw_staging_raw?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes

	 Ejemplo (autenticacion SQL):
	 DB_CONNECTION_STRING=mssql+pyodbc://usuario:password@localhost/dw_staging_raw?driver=ODBC+Driver+17+for+SQL+Server

	 Nota: para una instancia con nombre usar localhost%5CINSTANCIA o un puerto fijo localhost,PUERTO.

2) CSV_DIR
	 Ruta a la carpeta sources dentro del proyecto.
	 Por defecto: <raiz_del_proyecto>\sources
	 Si cambias la ubicacion, actualiza la variable en el script.

Entradas
--------
- t100_carriers_2023.csv (archivo principal)
- Varios CSVs de lookup (ver seccion "Mapeo de lookups")

Salidas
-------
- Tablas de staging en SQL Server con prefijo stg_.
- Mensajes en consola con el progreso y conteos.

Flujo general
-------------
1) Verifica que exista CSV_DIR.
2) Crea engine de SQLAlchemy y prueba conexion.
3) Carga lookups: trunca tabla y luego inserta filas.
4) Carga T100: trunca y carga por bloques de 1000 filas.
5) Verifica conteos de filas por tabla (stg_%).

Funciones principales
---------------------
- truncar_tabla(engine, tabla)
	Ejecuta TRUNCATE TABLE dbo.<tabla> para dejar la tabla vacia.

- cargar_lookups(engine, csv_dir)
	Recorre LOOKUP_MAP, lee cada CSV como texto, trunca y carga en la tabla.
	Si un archivo no existe, lo informa y lo salta.

- cargar_t100(engine, csv_dir)
	Lee t100_carriers_2023.csv, trunca stg_t100 y carga en bloques.

- verificar_carga(engine)
	Consulta sys.tables y sys.partitions para mostrar conteos por tabla stg_.

Mapeo de lookups
----------------
CSV -> Tabla staging

- l_unique_carriers.csv          -> stg_unique_carriers
- l_carrier_history.csv          -> stg_carrier_history
- l_carrier_group.csv            -> stg_carrier_group
- l_carrier_group_new.csv        -> stg_carrier_group_new
- l_strport.csv                  -> stg_airports
- l_strport_id.csv               -> stg_airport_ids
- l_strport_seq_id.csv           -> stg_airport_seq_ids
- l_country_code.csv             -> stg_country_codes
- l_world_area_codes.csv         -> stg_world_area_codes
- l_city_market_id.csv           -> stg_city_market_ids
- l_service_class.csv            -> stg_service_class
- l_strcraft_type.csv            -> stg_aircraft_type
- l_strcraft_group.csv           -> stg_aircraft_group
- l_strcraft_config.csv          -> stg_aircraft_config
- l_distance_group_500.csv       -> stg_distance_group
- l_months.csv                   -> stg_months
- l_quarters.csv                 -> stg_quarters
- l_region.csv                   -> stg_regions
- l_strline_id.csv               -> stg_airline_ids
- l_unique_carrier_entities.csv  -> stg_unique_carrier_entities

Ejecucion
---------
Desde la carpeta del proyecto:
		python extract.py

Notas de uso
------------
- Idempotencia: el script trunca las tablas antes de cada carga.
- Rendimiento: T100 se carga en bloques (chunksize=1000).
- Tipos: los lookups se leen como texto para evitar problemas de tipo.

Errores comunes
--------------
- Directorio CSV inexistente: revisar CSV_DIR.
- Error de conexion: revisar DB_CONNECTION_STRING en el .env, driver ODBC y credenciales.
- CSV faltante: el script lo reporta y continua con el resto.
