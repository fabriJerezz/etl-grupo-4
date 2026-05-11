"""
reglas_5_6_7.py
---------------
Limpieza y transformación de calidad de datos (Reglas 5, 6 y 7).
Reglas 5 y 6: cap in-place. Regla 7: imputar Distance por par Origin–Dest
si existe otro registro con distancia > 0; descartar solo lo irrecuperable.

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
from sqlalchemy import create_engine

# =============================================================
# 1. CONFIGURACIÓN Y CONEXIONES
# =============================================================
ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(ENV_PATH)

CONN_STR_STAGING = os.getenv("DB_CONNECTION_STRING")

engine_stg = create_engine(CONN_STR_STAGING, fast_executemany=True)

# Columnas T100 (BTS) para par aeropuerto origen–destino
COL_ORIGIN = "Origin"
COL_DEST = "Dest"

# =============================================================
# 2. FUNCIONES DE LIMPIEZA (Reglas de Calidad 5, 6 y 7)
# =============================================================

def aplicar_regla_5_dep_performed(df_datos):
    """
    Ajusta DepPerformed cuando supera a DepScheduled: lo iguala a DepScheduled.

    Problema 5: DepPerformed > DepScheduled (~480 registros en el dataset de práctica).
    Criterio ETL: se asume que el exceso se explica por reprogramaciones / reubicación
    de operaciones y que no se pierden salidas contables; se cap al programado.
    """
    print("  > Aplicando Regla 5 (DepPerformed > DepScheduled → DepPerformed := DepScheduled)")
    df_clean = df_datos.copy()

    dep_sched = pd.to_numeric(df_clean["DepScheduled"], errors="coerce")
    dep_perf = pd.to_numeric(df_clean["DepPerformed"], errors="coerce")

    mask = dep_perf.notna() & dep_sched.notna() & (dep_perf > dep_sched)
    n = int(mask.sum())
    if n:
        df_clean.loc[mask, "DepPerformed"] = dep_sched[mask]
    print(f"    - {n} registros ajustados (DepPerformed := DepScheduled).")
    return df_clean


def aplicar_regla_6_pasajeros_asientos(df_datos):
    """
    Ajusta Passengers cuando supera a Seats (solo si Seats > 0): lo iguala a Seats.

    Problema 6: Passengers > Seats (~560 registros en el dataset de práctica).
    Criterio ETL: se asume que el exceso puede deberse a cambio de asiento, revendedores,
    etc., y se cap a capacidad declarada. Seats = 0 no se toca aquí (otra regla / módulo).
    """
    print("  > Aplicando Regla 6 (Passengers > Seats → Passengers := Seats)")
    df_clean = df_datos.copy()

    passengers = pd.to_numeric(df_clean["Passengers"], errors="coerce")
    seats = pd.to_numeric(df_clean["Seats"], errors="coerce")

    mask = (
        passengers.notna()
        & seats.notna()
        & (seats > 0)
        & (passengers > seats)
    )
    n = int(mask.sum())
    if n:
        df_clean.loc[mask, "Passengers"] = seats[mask]
    print(f"    - {n} registros ajustados (Passengers := Seats).")
    return df_clean


def aplicar_regla_7_distancia(df_datos):
    """
    Si Distance es nula o <= 0, imputa con la distancia de otro registro del mismo
    par (Origin, Dest) que tenga Distance > 0 (referencia por OD en el mismo lote).

    Problema 7: Distance = 0 o negativa (~300 registros en el dataset de práctica).
    Si no hay ningún pariente con distancia válida para ese OD, la fila se descarta.
    """
    print(
        "  > Aplicando Regla 7 (Distance inválida → imputar por mismo Origin–Dest)..."
    )
    df_clean = df_datos.copy()

    if COL_ORIGIN not in df_clean.columns or COL_DEST not in df_clean.columns:
        raise KeyError(
            f"Faltan columnas {COL_ORIGIN!r} o {COL_DEST!r} para la regla 7."
        )

    distance = pd.to_numeric(df_clean["Distance"], errors="coerce")
    bad = distance.isna() | (distance <= 0)
    good = distance.notna() & (distance > 0)

    ref = (
        df_clean.loc[good, [COL_ORIGIN, COL_DEST]]
        .assign(_dist_ref=distance[good])
        .groupby([COL_ORIGIN, COL_DEST], as_index=False, dropna=False)["_dist_ref"]
        .max()
    )

    merged = df_clean.merge(ref, on=[COL_ORIGIN, COL_DEST], how="left")
    can_fill = bad & merged["_dist_ref"].notna()
    n_fill = int(can_fill.sum())
    if n_fill:
        merged.loc[can_fill, "Distance"] = merged.loc[can_fill, "_dist_ref"]
    merged = merged.drop(columns=["_dist_ref"])

    distance_after = pd.to_numeric(merged["Distance"], errors="coerce")
    still_bad = distance_after.isna() | (distance_after <= 0)
    n_drop = int(still_bad.sum())
    df_out = merged.loc[~still_bad].copy()

    print(f"    - {n_fill} registros con Distance imputada (mismo {COL_ORIGIN}–{COL_DEST}).")
    print(f"    - {n_drop} registros eliminados (sin referencia OD con distancia > 0).")
    return df_out


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
    print(
        f"  Registros descartados (regla 7, sin imputación posible): "
        f"{len(df_vuelos_crudos) - len(df_vuelos_limpios):,}"
    )
    print("  (Reglas 5 y 6: ajustes in-place; regla 7: imputa por OD y luego descarta irrecuperables.)")
    print("=" * 60)
