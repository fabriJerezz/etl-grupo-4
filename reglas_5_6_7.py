"""
reglas_5_6_7.py
---------------
Limpieza y transformación de calidad de datos (Reglas 5, 6 y 7).

REQUISITOS:
    pip install pandas sqlalchemy pyodbc python-dotenv

CONFIGURACIÓN:
    Crear un archivo .env con DB_CONNECTION_STRING.

USO:
    python reglas_5_6_7.py
"""

import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =============================================================
# 1. CONFIGURACIÓN Y CONEXIONES
# =============================================================
ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(ENV_PATH)

CONN_STR_STAGING = os.getenv("DB_CONNECTION_STRING")

engine_stg = create_engine(CONN_STR_STAGING, fast_executemany=True)

# =============================================================
# 2. FUNCIONES DE LIMPIEZA (Reglas de Calidad 5, 6 y 7)
# =============================================================

def aplicar_regla_5_dep_performed(df_datos):
    """
    Descarta registros donde los vuelos realizados superan los programados.

    Problema 5: DepPerformed > DepScheduled (~480 registros).
    Físicamente imposible: no pueden realizarse más vuelos de los planificados
    en un mismo período; indica error de carga o mezcla de períodos.
    """
    print("  > Aplicando Regla 5 (DepPerformed > DepScheduled)...")
    df_clean = df_datos.copy()
    filas_antes = len(df_clean)

    # Forzamos numérico para evitar fallos silenciosos por strings o NaN
    dep_sched = pd.to_numeric(df_clean["DepScheduled"], errors="coerce")
    dep_perf  = pd.to_numeric(df_clean["DepPerformed"],  errors="coerce")

    # Conservamos solo los registros físicamente válidos
    mask_validos = dep_perf <= dep_sched
    df_clean = df_clean[mask_validos].copy()

    descartados = filas_antes - len(df_clean)
    print(f"    - {descartados} registros eliminados por DepPerformed > DepScheduled.")
    return df_clean


def aplicar_regla_6_pasajeros_asientos(df_datos):
    """
    Descarta registros donde los pasajeros superan los asientos disponibles.

    Problema 6: Passengers > Seats (~560 registros).
    Violación de restricción de negocio: un vuelo no puede transportar más
    pasajeros que asientos. Excepción: si Seats = 0 el registro se conserva
    pero se marca con la columna 'flag_seats_cero' para revisión posterior.
    """
    print("  > Aplicando Regla 6 (Passengers > Seats)...")
    df_clean = df_datos.copy()
    filas_antes = len(df_clean)

    passengers = pd.to_numeric(df_clean["Passengers"], errors="coerce")
    seats      = pd.to_numeric(df_clean["Seats"],      errors="coerce")

    # Caso especial: Seats = 0 con Passengers > 0 → conservar y flagear
    mask_seats_cero = (seats == 0) & (passengers > 0)
    if "flag_seats_cero" not in df_clean.columns:
        df_clean["flag_seats_cero"] = False
    df_clean.loc[mask_seats_cero, "flag_seats_cero"] = True
    flagueados = mask_seats_cero.sum()

    # Descartar solo los registros con Seats > 0 y Passengers > Seats
    mask_invalidos = (seats > 0) & (passengers > seats)
    df_clean = df_clean[~mask_invalidos].copy()

    descartados = filas_antes - len(df_clean)
    print(f"    - {descartados} registros eliminados por Passengers > Seats.")
    print(f"    - {flagueados} registros con Seats = 0 conservados y marcados para revisión.")
    return df_clean


def aplicar_regla_7_distancia(df_datos):
    """
    Descarta registros donde la distancia entre aeropuertos es nula o negativa.

    Problema 7: Distance = 0 o negativa (~300 registros).
    Impide el cálculo de métricas de eficiencia (consumo por milla, velocidad,
    costo por kilómetro). Un segmento de vuelo siempre debe tener distancia > 0.
    """
    print("  > Aplicando Regla 7 (Distance <= 0)...")
    df_clean = df_datos.copy()
    filas_antes = len(df_clean)

    distance = pd.to_numeric(df_clean["Distance"], errors="coerce")

    df_clean = df_clean[distance > 0].copy()

    descartados = filas_antes - len(df_clean)
    print(f"    - {descartados} registros eliminados por Distance <= 0.")
    return df_clean


# =============================================================
# 3. ORQUESTADOR
# =============================================================

def limpiar_calidad_5_6_7(df_datos):
    """
    Ejecuta las reglas 5, 6 y 7 en cadena sobre el DataFrame recibido.
    """
    print("\nIniciando módulo de calidad (Reglas 5, 6 y 7)...")
    df_procesado = df_datos.copy()

    df_procesado = aplicar_regla_5_dep_performed(df_procesado)
    df_procesado = aplicar_regla_6_pasajeros_asientos(df_procesado)
    df_procesado = aplicar_regla_7_distancia(df_procesado)

    print("  ✓ Módulo de calidad completado.\n")
    return df_procesado


# =============================================================
# 4. EXTRACCIÓN Y EJECUCIÓN
# =============================================================

if __name__ == "__main__":
    print("=" * 60)
    print("1. Extrayendo datos desde dw_staging_raw...")
    print("=" * 60)

    query_stg = "SELECT * FROM stg_t100"
    df_vuelos_crudos = pd.read_sql(query_stg, engine_stg)
    print(f"✓ Datos extraídos: {len(df_vuelos_crudos):,} registros en total.")

    df_vuelos_limpios = limpiar_calidad_5_6_7(df_vuelos_crudos)

    print("=" * 60)
    print("Resumen:")
    print(f"  Registros iniciales:              {len(df_vuelos_crudos):,}")
    print(f"  Registros listos para continuar:  {len(df_vuelos_limpios):,}")
    print(f"  Registros descartados:            {len(df_vuelos_crudos) - len(df_vuelos_limpios):,}")
    if "flag_seats_cero" in df_vuelos_limpios.columns:
        flagueados = df_vuelos_limpios["flag_seats_cero"].sum()
        print(f"  Registros flagueados (Seats=0):   {flagueados:,}")
    print("=" * 60)
