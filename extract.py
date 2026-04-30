"""
02_cargar_staging.py
--------------------
Carga los CSVs fuente en las tablas de staging de SQL Server.

REQUISITOS:
    pip install pandas sqlalchemy pyodbc

CONFIGURACIÓN:
    Modificar la variable CONNECTION_STRING con los datos de tu servidor.

USO:
    python 02_cargar_staging.py

NOTA:
    - El directorio CSV_DIR debe apuntar a la carpeta donde descomprimiste
      el ZIP con los archivos fuente (SourcesAerolineas).
    - El script es idempotente: trunca las tablas antes de cargar,
      así podés re-ejecutarlo sin generar duplicados.
"""

import pandas as pd
from sqlalchemy import create_engine, text
import time
import os

# =============================================================
# CONFIGURACIÓN - MODIFICAR SEGÚN TU ENTORNO
# =============================================================

# Formato: mssql+pyodbc://usuario:password@servidor/base?driver=ODBC+Driver+17+for+SQL+Server
# Si usás autenticación Windows, dejá usuario y password vacíos:
CONNECTION_STRING = (
    "mssql+pyodbc://@localhost/dw_staging"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)

# Ruta a la carpeta con los CSVs extraídos
CSV_DIR = r"C:\SourcesAerolineas"  # <-- CAMBIAR a tu ruta local

# =============================================================
# MAPEO: archivo CSV -> tabla staging
# =============================================================
LOOKUP_MAP = {
    "l_unique_carriers.csv":          "stg_unique_carriers",
    "l_carrier_history.csv":          "stg_carrier_history",
    "l_carrier_group.csv":            "stg_carrier_group",
    "l_carrier_group_new.csv":        "stg_carrier_group_new",
    "l_strport.csv":                  "stg_airports",
    "l_strport_id.csv":              "stg_airport_ids",
    "l_strport_seq_id.csv":          "stg_airport_seq_ids",
    "l_country_code.csv":            "stg_country_codes",
    "l_world_area_codes.csv":        "stg_world_area_codes",
    "l_city_market_id.csv":          "stg_city_market_ids",
    "l_service_class.csv":           "stg_service_class",
    "l_strcraft_type.csv":           "stg_aircraft_type",
    "l_strcraft_group.csv":          "stg_aircraft_group",
    "l_strcraft_config.csv":         "stg_aircraft_config",
    "l_distance_group_500.csv":      "stg_distance_group",
    "l_months.csv":                  "stg_months",
    "l_quarters.csv":                "stg_quarters",
    "l_region.csv":                  "stg_regions",
    "l_strline_id.csv":              "stg_airline_ids",
    "l_unique_carrier_entities.csv": "stg_unique_carrier_entities",
}

# =============================================================
# FUNCIONES
# =============================================================

def truncar_tabla(engine, tabla):
    """Vacía la tabla antes de cargar (idempotencia)."""
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE dbo.{tabla}"))
        conn.commit()


def cargar_t100(engine, csv_dir):
    """Carga el archivo T100 principal en stg_t100."""
    archivo = os.path.join(csv_dir, "t100_carriers_2023.csv")
    tabla = "stg_t100"

    print(f"\n{'='*60}")
    print(f"Cargando {archivo} -> {tabla}")
    print(f"{'='*60}")

    inicio = time.time()

    # Leer CSV
    df = pd.read_csv(archivo, encoding="utf-8")
    print(f"  Filas leídas del CSV: {len(df):,}")

    # Truncar tabla destino
    truncar_tabla(engine, tabla)

    # Cargar en bloques de 5000 filas (más eficiente para tablas grandes)
    df.to_sql(
        name=tabla,
        con=engine,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi"
    )

    elapsed = time.time() - inicio
    print(f"  ✓ {len(df):,} filas cargadas en {elapsed:.1f}s")
    return len(df)


def cargar_lookups(engine, csv_dir):
    """Carga todos los archivos lookup en sus tablas staging."""
    total_filas = 0

    print(f"\n{'='*60}")
    print(f"Cargando lookups")
    print(f"{'='*60}")

    for csv_file, tabla in LOOKUP_MAP.items():
        archivo = os.path.join(csv_dir, csv_file)

        if not os.path.exists(archivo):
            print(f"  ⚠ No encontrado: {csv_file} -- saltando")
            continue

        # Leer CSV (todo como string para evitar problemas de tipo)
        df = pd.read_csv(archivo, dtype=str, encoding="utf-8")

        # Truncar tabla destino
        truncar_tabla(engine, tabla)

        # Cargar
        df.to_sql(
            name=tabla,
            con=engine,
            if_exists="append",
            index=False,
        )

        print(f"  ✓ {csv_file:40s} -> {tabla:30s} ({len(df)} filas)")
        total_filas += len(df)

    return total_filas


def verificar_carga(engine):
    """Muestra el conteo de filas de cada tabla staging."""
    print(f"\n{'='*60}")
    print(f"VERIFICACIÓN - Conteo de filas por tabla")
    print(f"{'='*60}")

    query = """
        SELECT t.name AS tabla, 
               SUM(p.rows) AS filas
        FROM sys.tables t
        JOIN sys.partitions p ON t.object_id = p.object_id
        WHERE p.index_id IN (0, 1)
          AND t.name LIKE 'stg_%'
        GROUP BY t.name
        ORDER BY t.name
    """

    with engine.connect() as conn:
        result = conn.execute(text(query))
        total = 0
        for row in result:
            print(f"  {row[0]:40s} {row[1]:>10,} filas")
            total += row[1]
        print(f"  {'─'*52}")
        print(f"  {'TOTAL':40s} {total:>10,} filas")


# =============================================================
# EJECUCIÓN PRINCIPAL
# =============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("ETL - CARGA DE STAGING AREA")
    print("=" * 60)

    # Verificar que el directorio de CSVs existe
    if not os.path.isdir(CSV_DIR):
        print(f"\n❌ ERROR: No se encontró el directorio: {CSV_DIR}")
        print(f"   Modificá la variable CSV_DIR en el script.")
        exit(1)

    # Crear conexión
    print(f"\nConectando a SQL Server...")
    engine = create_engine(CONNECTION_STRING, fast_executemany=True)

    # Verificar conexión
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print(f"  ✓ Conexión exitosa")
    except Exception as e:
        print(f"\nX ERROR de conexión: {e}")
        print(f"   Verificá CONNECTION_STRING en el script.")
        exit(1)

    inicio_total = time.time()

    # 1. Cargar lookups
    filas_lookups = cargar_lookups(engine, CSV_DIR)

    # 2. Cargar T100 (el más grande)
    filas_t100 = cargar_t100(engine, CSV_DIR)

    # 3. Verificar
    verificar_carga(engine)

    elapsed_total = time.time() - inicio_total
    print(f"\n{'='*60}")
    print(f"CARGA COMPLETADA")
    print(f"  Lookups: {filas_lookups:,} filas")
    print(f"  T100:    {filas_t100:,} filas")
    print(f"  Tiempo:  {elapsed_total:.1f}s")
    print(f"{'='*60}")