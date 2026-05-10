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

# ------------------------------------------------------------------
# Regla 9 – mapeo de AirlineID "absorbente" por código IATA reutilizado
# Clave  : UniqueCarrier cuyo código fue heredado post-fusión
# Valor  : dict con los AirlineID que corresponden al carrier ABSORBENTE
#          y el código correcto del absorbente para normalizar (opcional).
#
# Fuente histórica BTS:
#   CO (Continental) → absorbido por UA (United)   AirlineID absorbente: 19977
#   US (US Airways)  → absorbido por AA (American) AirlineID absorbente: 19805
#   FL (AirTran)     → absorbido por WN (Southwest) AirlineID absorbente: 19393
#   VX (Virgin America) → absorbida por AS (Alaska) AirlineID absorbente: 19690
# ------------------------------------------------------------------
MERGED_CARRIERS = {
    "CO": {"absorbed_by_ids": {19977}, "successor_code": "UA"},
    "US": {"absorbed_by_ids": {19805}, "successor_code": "AA"},
    "FL": {"absorbed_by_ids": {19393}, "successor_code": "WN"},
    "VX": {"absorbed_by_ids": {19690}, "successor_code": "AS"},
}

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
# REGLA 9 – Código IATA reutilizado post-fusión
# =============================================================
"""
def regla_9_iata_reutilizado(df: pd.DataFrame) -> pd.DataFrame:
    """"""
    Detecta registros donde el mismo UniqueCarrier (ej. 'CO', 'US', 'FL', 'VX')
    aparece con el AirlineID del carrier absorbente —indicando que el código
    fue heredado temporalmente post-fusión— y agrega la columna:

        is_merged_carrier (bool)  → True cuando el registro está afectado.

    Opcionalmente también corrige UniqueCarrier al código del absorbente
    (línea comentada, habilitar si se prefiere normalizar en lugar de flaggear).
    """"""
    antes = df.shape[0]

    df["is_merged_carrier"] = False
    total_afectados = 0

    for carrier, info in MERGED_CARRIERS.items():
        absorbed_ids  = info["absorbed_by_ids"]
        # successor     = info["successor_code"]   # descomentar si se normaliza

        mascara = (
            (df["UniqueCarrier"] == carrier) &
            (df["AirlineID"].isin(absorbed_ids))
        )
        n = mascara.sum()
        if n:
            df.loc[mascara, "is_merged_carrier"] = True
            # df.loc[mascara, "UniqueCarrier"] = successor  # normalización opcional
            print(f"    {carrier} con AirlineID {absorbed_ids}: {n:,} registros flaggeados")
            total_afectados += n

    log_cambio(
        "Regla 9 – IATA reutilizado (columna is_merged_carrier agregada)",
        antes, df.shape[0],
        f"Total registros flaggeados: {total_afectados:,}  "
        f"(sin eliminación de filas)"
    )
    return df
"""

# =============================================================
# REGLA 9 – Detección Dinámica de Códigos IATA reutilizados
# =============================================================

"""def regla_9_iata_reutilizado(df: pd.DataFrame) -> pd.DataFrame:
    """
"""
    Detecta dinámicamente si un UniqueCarrier está asociado a más de un AirlineID.
    Asume que el AirlineID con mayor cantidad de vuelos es el 'principal'.
    Marca con is_merged_carrier=True a los registros que utilizan los AirlineID 
    minoritarios (anómalos/post-fusión).  
    """
"""
    antes = df.shape[0]
    df["is_merged_carrier"] = False
    total_afectados = 0

    # 1. Contar cuántos vuelos hay por cada combinación de Carrier e ID
    freq = df.groupby(["UniqueCarrier", "AirlineID"]).size().reset_index(name="vuelos")

    # 2. Filtrar los UniqueCarrier que tienen más de un AirlineID distinto
    conteo_ids_por_carrier = freq.groupby("UniqueCarrier").size()
    carriers_anomalos = conteo_ids_por_carrier[conteo_ids_por_carrier > 1].index

    # 3. Evaluar cada carrier anómalo
    for carrier in carriers_anomalos:
        # Ordenar los IDs de este carrier de mayor a menor cantidad de vuelos
        ids_del_carrier = freq[freq["UniqueCarrier"] == carrier].sort_values("vuelos", ascending=False)
        
        id_principal = ids_del_carrier.iloc[0]["AirlineID"]
        ids_minoritarios = ids_del_carrier.iloc[1:]["AirlineID"].tolist()

        # Flaggear los registros que tienen los IDs minoritarios
        mascara = (df["UniqueCarrier"] == carrier) & (df["AirlineID"].isin(ids_minoritarios))
        n_flaggeados = mascara.sum()
        
        if n_flaggeados > 0:
            df.loc[mascara, "is_merged_carrier"] = True
            print(f"    {carrier}: Principal ID {id_principal}. Flaggeados {n_flaggeados:,} vols con IDs secundarios {ids_minoritarios}")
            total_afectados += n_flaggeados

    log_cambio(
        "Regla 9 – IATA reutilizado (Detección Dinámica)",
        antes, df.shape[0],
        f"Total registros flaggeados: {total_afectados:,} (sin eliminación de filas)"
    )
    return df"""

# =============================================================
# REGLA 9 – Código IATA reutilizado post-fusión (Actualización Dinámica)
# =============================================================

def regla_9_iata_reutilizado(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detecta dinámicamente si un AirlineID está siendo usado por un código 
    UniqueCarrier obsoleto (post-fusión).
    Encuentra al 'dueño real' de ese AirlineID y actualiza la fila 
    sobrescribiendo el UniqueCarrier obsoleto con el correcto.
    """
    filas_trabajadas = df.shape[0]

    # 1. Determinar el "dueño real" de cada AirlineID
    # Agrupamos por ID y Carrier para contar cuántos vuelos tiene cada combinación
    uso_ids = df.groupby(["AirlineID", "UniqueCarrier"]).size().reset_index(name="vuelos")
    
    # Ordenamos de mayor a menor y nos quedamos solo con el Carrier que más usa cada ID
    dueños_reales = uso_ids.sort_values("vuelos", ascending=False).drop_duplicates(subset=["AirlineID"])
    
    # Creamos un diccionario mapeando {AirlineID: UniqueCarrier_Principal}
    mapa_dueños = dict(zip(dueños_reales["AirlineID"], dueños_reales["UniqueCarrier"]))

    # 2. Identificar las filas anómalas
    # Buscamos qué UniqueCarrier debería tener cada fila según su AirlineID
    carrier_correcto = df["AirlineID"].map(mapa_dueños)
    
    # Una fila es anómala si su Carrier actual NO coincide con el dueño real de su AirlineID
    mascara_anomala = (df["UniqueCarrier"] != carrier_correcto) & carrier_correcto.notna()
    
    filas_afectadas = mascara_anomala.sum()

    # 3. Actualizar (sobrescribir) los datos en el DataFrame
    if filas_afectadas > 0:
        # Mostramos un detalle por consola de qué reemplazos se están haciendo
        cambios = df[mascara_anomala].groupby(["UniqueCarrier", carrier_correcto[mascara_anomala]]).size()
        print("\n    Detalle de fusiones actualizadas:")
        for (viejo, nuevo), cant in cambios.items():
            print(f"      - {viejo} actualizado a {nuevo} ({cant:,} vuelos)")
            
        # Aplicamos la actualización real en las filas
        df.loc[mascara_anomala, "UniqueCarrier"] = carrier_correcto[mascara_anomala]

    log_cambio(
        regla="Regla 9 – IATA reutilizado (Actualización Dinámica)",
        procesadas=filas_trabajadas,
        afectadas=filas_afectadas,
        resultantes=df.shape[0],
        accion="actualizadas"
    )
    
    # Opcional: eliminar la columna is_merged_carrier de la función anterior si la tenías
    if "is_merged_carrier" in df.columns:
        df = df.drop(columns=["is_merged_carrier"])

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
