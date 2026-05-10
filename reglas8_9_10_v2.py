"""
03_transform_staging.py
-----------------------
Lee stg_t100 desde SQL Server, aplica reglas de calidad/transformación
y deja el DataFrame limpio listo para cargarse al Data Warehouse.

REGLAS APLICADAS:
    Regla 8  - AirTime / RampTime imposibles  → valores anómalos a NaN
    Regla 9  - Código IATA reutilizado        → flag is_merged_carrier
    Regla 10 - Filas duplicadas exactas       → se elimina la duplicada

REQUISITOS:
    pip install pandas sqlalchemy pyodbc python-dotenv

CONFIGURACIÓN:
    Archivo .env en la raíz del proyecto con DB_CONNECTION_STRING.

USO:
    python transform.py
"""

import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =============================================================
# CONFIGURACIÓN
# =============================================================

ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(ENV_PATH)

CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# Tabla de origen en staging
STG_TABLE = "stg_t100"

# Umbrales para Regla 8
AIRTIME_MIN   =    1      # minutos – mínimo razonable
AIRTIME_MAX   = 9000      # minutos – máximo razonable
RAMPTIME_MIN  =    1
RAMPTIME_MAX  = 9000


# =============================================================
# UTILIDADES DE LOG
# =============================================================

def banner(titulo: str) -> None:
    linea = "=" * 60
    print(f"\n{linea}")
    print(f"  {titulo}")
    print(linea)


def log_cambio(regla: str, antes: int, despues: int, detalle: str = "") -> None:
    delta = antes - despues
    pct   = (delta / antes * 100) if antes else 0
    print(f"\n  ► {regla}")
    print(f"    Filas antes  : {antes:>10,}")
    print(f"    Filas después: {despues:>10,}")
    print(f"    Δ filas      : {delta:>10,}  ({pct:.2f}%)")
    if detalle:
        print(f"    {detalle}")


# =============================================================
# REGLA 8 – AirTime / RampTime imposibles
# =============================================================

def regla_8_tiempos_imposibles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte a NaN los valores de AirTime y RampTime que sean:
      - negativos
      - cero
      - mayores a 9 000 minutos
    No elimina la fila: el resto del registro sigue siendo válido.
    """
    antes = df.shape[0]

    for col, col_min, col_max in [
        ("AirTime",  AIRTIME_MIN,  AIRTIME_MAX),
        ("RampTime", RAMPTIME_MIN, RAMPTIME_MAX),
    ]:
        if col not in df.columns:
            print(f"    ⚠ Columna '{col}' no encontrada – saltando.")
            continue

        mascara_anomala = (
            df[col].notna() &
            ((df[col] < col_min) | (df[col] > col_max))
        )
        n_anomalos = mascara_anomala.sum()
        df.loc[mascara_anomala, col] = np.nan
        print(f"    {col}: {n_anomalos:,} valores anómalos → NaN")

    log_cambio(
        "Regla 8 – Tiempos imposibles (NaN, no eliminación de filas)",
        antes, df.shape[0],
        "AirTime y RampTime fuera de rango marcados como NaN."
    )
    return df


# =============================================================
# REGLA 9 – Código IATA reutilizado (detección dinámica)
# =============================================================

def _detectar_carriers_con_multiples_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa por UniqueCarrier y cuenta los AirlineID distintos asociados.
    Devuelve un DataFrame con:
        UniqueCarrier | ids_distintos | id_dominante | conteo_dominante
    Solo incluye carriers que tienen MÁS DE UN AirlineID (casos anómalos).

    El AirlineID dominante se determina por la cantidad de registros
    (si hay empate, se usa como criterio secundario la suma de Passengers).
    """
    # Contar registros por (UniqueCarrier, AirlineID)
    conteos = (
        df.groupby(["UniqueCarrier", "AirlineID"], as_index=False)
        .agg(
            n_registros=("AirlineID", "count"),
            n_pasajeros=("Passengers", "sum"),
        )
    )

    # Carriers con más de un AirlineID
    ids_por_carrier = conteos.groupby("UniqueCarrier")["AirlineID"].nunique()
    carriers_anomalos = ids_por_carrier[ids_por_carrier > 1].index

    if len(carriers_anomalos) == 0:
        return pd.DataFrame(
            columns=["UniqueCarrier", "ids_distintos", "id_dominante", "conteo_dominante"]
        )

    # Para cada carrier anómalo, determinar el AirlineID dominante
    subset = conteos[conteos["UniqueCarrier"].isin(carriers_anomalos)].copy()

    # Ordenar: mayor n_registros primero; en empate, mayor n_pasajeros
    subset = subset.sort_values(
        ["UniqueCarrier", "n_registros", "n_pasajeros"],
        ascending=[True, False, False],
    )

    dominante = subset.groupby("UniqueCarrier").first().reset_index()
    dominante = dominante.rename(columns={
        "AirlineID":    "id_dominante",
        "n_registros":  "conteo_dominante",
        "n_pasajeros":  "pasajeros_dominante",
    })

    # Agregar cuántos IDs distintos tiene cada carrier anómalo
    dominante["ids_distintos"] = dominante["UniqueCarrier"].map(ids_por_carrier)

    return dominante[["UniqueCarrier", "ids_distintos", "id_dominante",
                       "conteo_dominante", "pasajeros_dominante"]]


def regla_9_iata_reutilizado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detección dinámica de códigos IATA reutilizados post-fusión.

    Lógica:
      1. Agrupa los datos y detecta cualquier UniqueCarrier asociado a
         más de un AirlineID → indica reutilización del código.
      2. Determina el AirlineID dominante (más registros; desempate por
         total de pasajeros).
      3. Para cada registro de esos carriers:
         - Asigna is_merged_carrier = True si el AirlineID NO es el dominante.
         - Reemplaza AirlineID por el dominante en esos registros
           (estandarización), conservando el valor original en
           AirlineID_original para trazabilidad.

    El resultado mantiene todas las filas; solo se corrige AirlineID
    en los registros anómalos y se agrega la columna flag.
    """
    antes = df.shape[0]

    # ── Paso 1: detectar carriers anómalos ──────────────────────
    anomalos = _detectar_carriers_con_multiples_ids(df)

    df["is_merged_carrier"] = False
    df["AirlineID_original"] = df["AirlineID"]   # trazabilidad

    if anomalos.empty:
        print("    ✓ No se detectaron carriers con múltiples AirlineID.")
        log_cambio(
            "Regla 9 – IATA reutilizado (detección dinámica)",
            antes, df.shape[0],
            "Sin anomalías detectadas."
        )
        return df

    # ── Paso 2: log de hallazgos ─────────────────────────────────
    print(f"\n    Carriers con múltiples AirlineID detectados: "
          f"{len(anomalos)}")
    print(f"    {'UniqueCarrier':<16} {'IDs distintos':>14} "
          f"{'ID dominante':>14} {'Registros dominante':>20}")
    print(f"    {'─'*66}")
    for _, row in anomalos.iterrows():
        print(
            f"    {row['UniqueCarrier']:<16} "
            f"{int(row['ids_distintos']):>14} "
            f"{int(row['id_dominante']):>14} "
            f"{int(row['conteo_dominante']):>20,}"
        )

    # ── Paso 3: estandarizar y flaggear ─────────────────────────
    total_afectados = 0

    for _, row in anomalos.iterrows():
        carrier      = row["UniqueCarrier"]
        id_dominante = int(row["id_dominante"])

        # Registros de este carrier que NO tienen el AirlineID dominante
        mascara_anomala = (
            (df["UniqueCarrier"] == carrier) &
            (df["AirlineID"] != id_dominante)
        )
        n = mascara_anomala.sum()

        if n:
            df.loc[mascara_anomala, "is_merged_carrier"] = True
            df.loc[mascara_anomala, "AirlineID"] = id_dominante
            total_afectados += n

    print(f"\n    Total registros corregidos y flaggeados: {total_afectados:,}")

    log_cambio(
        "Regla 9 – IATA reutilizado (detección dinámica)",
        antes, df.shape[0],
        f"AirlineID estandarizado al dominante en {total_afectados:,} registros. "
        f"Valor original preservado en 'AirlineID_original'."
    )
    return df


# =============================================================
# REGLA 10 – Duplicados exactos
# =============================================================

def regla_10_duplicados(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina filas que son duplicados exactos en TODAS las columnas
    (incluyendo la nueva is_merged_carrier), conservando la primera ocurrencia.
    """
    antes = df.shape[0]

    df = df.drop_duplicates(keep="first")

    log_cambio(
        "Regla 10 – Duplicados exactos eliminados",
        antes, df.shape[0],
        "Se conservó la primera ocurrencia de cada duplicado."
    )
    return df


# =============================================================
# PIPELINE PRINCIPAL
# =============================================================

def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica las tres reglas en orden."""
    banner("INICIO TRANSFORMACIÓN")
    print(f"\n  Filas cargadas desde staging: {df.shape[0]:,}")
    print(f"  Columnas                     : {df.shape[1]}")

    df = regla_8_tiempos_imposibles(df)
    df = regla_9_iata_reutilizado(df)
    df = regla_10_duplicados(df)

    return df


# =============================================================
# EJECUCIÓN
# =============================================================

if __name__ == "__main__":
    banner("ETL – TRANSFORMACIÓN DE stg_t100")

    # ── Conexión ──────────────────────────────────────────────
    if not CONNECTION_STRING:
        print("\nX ERROR: No se encontró DB_CONNECTION_STRING en el .env")
        exit(1)

    print("\nConectando a SQL Server...")
    engine = create_engine(CONNECTION_STRING, fast_executemany=True)

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("  ✓ Conexión exitosa")
    except Exception as e:
        print(f"\nX ERROR de conexión: {e}")
        exit(1)

    # ── Lectura ───────────────────────────────────────────────
    print(f"\nLeyendo {STG_TABLE}...")
    df_raw = pd.read_sql(f"SELECT * FROM dbo.{STG_TABLE}", con=engine)
    print(f"  ✓ {len(df_raw):,} filas leídas ({df_raw.shape[1]} columnas)")

    # ── Transformación ────────────────────────────────────────
    df_clean = transformar(df_raw)

    # ── Resumen final ─────────────────────────────────────────
    banner("RESUMEN FINAL")
    print(f"\n  Filas originales      : {len(df_raw):,}")
    print(f"  Filas tras limpieza   : {len(df_clean):,}")
    print(f"  Filas eliminadas      : {len(df_raw) - len(df_clean):,}")
    print(f"  NaN en AirTime        : {df_clean['AirTime'].isna().sum():,}")
    print(f"  NaN en RampTime       : {df_clean['RampTime'].isna().sum():,}")
    print(f"  is_merged_carrier=True: {df_clean['is_merged_carrier'].sum():,}")
    print(f"  AirlineID corregidos  : {(df_clean['AirlineID'] != df_clean['AirlineID_original']).sum():,}")
    print(f"\n  DataFrame listo para carga al DW → df_clean")
    print("=" * 60)

    # df_clean queda disponible para el siguiente paso del ETL.
    # Ejemplo de carga al DW (descomentar cuando esté disponible el engine del DW):
    #
    # df_clean.to_sql(
    #     name="fact_t100",
    #     con=engine_dw,
    #     if_exists="append",
    #     index=False,
    #     chunksize=1000,
    # )
