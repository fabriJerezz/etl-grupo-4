"""
transform_rules_2_3_4.py
------------
Fase de transformación del ETL. Lee las tablas de staging desde SQL Server,
aplica las reglas de calidad asignadas (2, 3 y 4) y retorna DataFrames limpios listos
para la fase de carga (load.py).

REGLAS IMPLEMENTADAS:
    R02 - Normalización de UniqueCarrierName
    R03 - AirlineID = 0 o NULL  →  "Desconocido"
    R04 - Campos obligatorios vacíos  →  detección y reemplazo con NULL

REQUISITOS:
    pip install pandas sqlalchemy pyodbc python-dotenv

CONFIGURACIÓN:
    Archivo .env con DB_CONNECTION_STRING en la raíz del proyecto.

USO (como módulo):
    from transform import aplicar_reglas
    df_t100, df_resumen = aplicar_reglas(engine)

USO (directo):
    python transform_rules_2_3_4.py
"""

import os
import re

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =============================================================
# CONFIGURACIÓN
# =============================================================

ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(ENV_PATH)

CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# Campos de stg_t100 que no deben estar vacíos
CAMPOS_OBLIGATORIOS = [
    "UniqueCarrierName",
    "CarrierName",
    "AircraftType",
    "OriginCityName",
    "DestCityName",
    "Class",
]

# Sufijos corporativos a quitar del nombre del carrier (R02)
SUFIJOS_CORPORATIVOS = [
    r",?\s*INC\.?",
    r",?\s*CO\.?",
    r",?\s*LTD\.?",
    r",?\s*LLC\.?",
    r",?\s*CORP\.?",
    r",?\s*S\.A\.?",
    r",?\s*LIMITED",
]

PATRON_SUFIJOS = re.compile(
    "(" + "|".join(SUFIJOS_CORPORATIVOS) + r")\s*$",
    flags=re.IGNORECASE,
)

# Abreviaturas de palabras sueltas dentro del nombre (R02)
# Se aplican DESPUÉS de quitar sufijos, palabra por palabra.
ABREVIATURAS = {
    "AMER.":  "AMERICAN",
    "AMER":   "AMERICAN",
    "AIRLS.": "AIR LINES",
    "AIRLS":  "AIR LINES",
    "INTL.":  "INTERNATIONAL",
    "INTL":   "INTERNATIONAL",
}

# Mapa de nombres canónicos (R02)
# Clave: nombre ya en UPPER, sin sufijos, con abreviaturas expandidas.
# Valor: nombre oficial definitivo.
NOMBRES_CANONICOS = {
    # AMERICAN AIRLINES
    "AMERICANAIRLINES":       "AMERICAN AIRLINES",

    # DELTA AIR LINES  (AIRLINES → AIR LINES)
    "DELTA AIRLINES":         "DELTA AIR LINES",

    # UNITED AIRLINES  (AIR LINES → AIRLINES)
    "UNITED AIR LINES":       "UNITED AIRLINES",

    # SOUTHWEST AIRLINES  (queda "SOUTHWEST AIR" tras quitar CO)
    "SOUTHWEST AIR":          "SOUTHWEST AIRLINES",

    # ALASKA AIRLINES
    "ALASKA AIR LINES":       "ALASKA AIRLINES",
    "ALASKA AIR":             "ALASKA AIRLINES",

    # JETBLUE AIRWAYS
    "JETBLUE":                "JETBLUE AIRWAYS",
    "JET BLUE AIRWAYS":       "JETBLUE AIRWAYS",
    "JET BLUE":               "JETBLUE AIRWAYS",

    # FRONTIER AIRLINES
    "FRONTIER AIR LINES":     "FRONTIER AIRLINES",
    "FRONTIER AIR":           "FRONTIER AIRLINES",

    # ALLEGIANT AIR
    "ALLEGIANT AIRLINES":     "ALLEGIANT AIR",

    # SPIRIT AIR LINES  (AIRLINES → AIR LINES)
    "SPIRIT AIRLINES":        "SPIRIT AIR LINES",

    # SKYWEST AIRLINES
    "SKYWEST AIR LINES":      "SKYWEST AIRLINES",
    "SKY WEST AIRLINES":      "SKYWEST AIRLINES",
    "SKY WEST AIR LINES":     "SKYWEST AIRLINES",

    # REPUBLIC AIRLINES
    "REPUBLIC AIR LINES":     "REPUBLIC AIRLINES",
    "REPUBLIC AIR":           "REPUBLIC AIRLINES",

    # ENDEAVOR AIR
    "ENDEAVOR AIRLINES":      "ENDEAVOR AIR",
    "ENDEAVOR AIR LINES":     "ENDEAVOR AIR",

    # ENVOY AIR
    "ENVOY AIRLINES":         "ENVOY AIR",
    "AMERICAN EAGLE (ENVOY)": "ENVOY AIR",

    # MESA AIR
    "MESA AIRLINES":          "MESA AIR",

    # COMMUT AIR  (CommutAir escrito junto)
    "COMMUTAIR":              "COMMUT AIR",
}

# =============================================================
# R02 — NORMALIZACIÓN DE UniqueCarrierName
# =============================================================

def normalizar_nombre_carrier(nombre: str) -> str:
    """Aplica R02: UPPER + quita sufijos + expande abreviaturas + nombre canónico."""
    if not isinstance(nombre, str):
        return nombre
    # 1. Estandarizar capitalización y espacios
    nombre = nombre.strip().upper()
    # 2. Quitar sufijos corporativos del final (INC., CO., LLC, LIMITED, etc.)
    nombre = PATRON_SUFIJOS.sub("", nombre).strip()
    # 3. Expandir abreviaturas palabra por palabra (AIRLS → AIR LINES, AMER → AMERICAN)
    nombre = " ".join(ABREVIATURAS.get(p, p) for p in nombre.split())
    # 4. Mapear al nombre canónico; si no está en el mapa, conservar el resultado anterior
    return NOMBRES_CANONICOS.get(nombre, nombre)


def aplicar_r02(df: pd.DataFrame) -> pd.DataFrame:
    """
    R02: Normaliza UniqueCarrierName.

    Problema: el mismo carrier aparece con nombres distintos según el mes/reporte:
      "American Airlines Inc." / "AMERICAN AIRLINES INC." / "Amer. Airlines Inc."
    Solución: UPPER + STRIP + quitar sufijos legales comunes.
    
    Ejemplo: "American Airlines Inc." → "AMERICAN AIRLINES" → "AMERICAN AIRLINES" (canónico)

    Columna afectada: UniqueCarrierName
    """
    print("  [R02] Normalizando UniqueCarrierName...")

    antes = df["UniqueCarrierName"].nunique()
    df = df.copy()
    df["UniqueCarrierName"] = df["UniqueCarrierName"].apply(normalizar_nombre_carrier)
    despues = df["UniqueCarrierName"].nunique()

    print(f"         Valores únicos antes: {antes:,}  →  después: {despues:,}")
    return df


# =============================================================
# R03 — AirlineID = 0 O NULL  →  "Desconocido"
# =============================================================

AIRLINE_ID_DESCONOCIDO = 0
CARRIER_CODE_DESCONOCIDO = "UNK"
CARRIER_NOMBRE_DESCONOCIDO = "DESCONOCIDO"


def aplicar_r03(df: pd.DataFrame) -> pd.DataFrame:
    """
    R03: Trata AirlineID inválido (0 o NULL) como carrier "Desconocido".

    Problema: ~350 registros tienen AirlineID = 0 (carriers extintos,
              archivos históricos pre-2000). Impide joins con dimensiones.
    Solución: reemplazar AirlineID = 0 por valores centinela homogéneos para que
              la fase de carga pueda mapearlo al registro "Desconocido".

    Columnas afectadas: AirlineID, UniqueCarrier, UniqueCarrierName
    """
    print("  [R03] Tratando AirlineID = 0 ...")

    df = df.copy()
    mascara = df["AirlineID"] == 0
    cantidad = mascara.sum()

    df.loc[mascara, "AirlineID"] = AIRLINE_ID_DESCONOCIDO
    df.loc[mascara, "UniqueCarrier"] = CARRIER_CODE_DESCONOCIDO
    df.loc[mascara, "UniqueCarrierName"] = CARRIER_NOMBRE_DESCONOCIDO

    print(f"         Registros marcados como Desconocido: {cantidad:,}")
    return df


# =============================================================
# R04 — CAMPOS OBLIGATORIOS VACÍOS  →  NULL
# =============================================================

def _es_vacio(valor) -> bool:
    """True si el valor es None, NaN o string en blanco."""
    if valor is None:
        return True
    if isinstance(valor, float) and pd.isna(valor):
        return True
    if isinstance(valor, str) and valor.strip() == "":
        return True
    return False


def aplicar_r04(df: pd.DataFrame) -> pd.DataFrame:
    """
    R04: Detecta strings vacíos en campos obligatorios y los convierte a NULL.

    Problema: ~700 registros tienen strings vacíos ("") en campos que deberían
              tener valor. Un string vacío no es NULL: pasa filtros IS NOT NULL
              y genera dimensiones fantasma en la BD destino.
    Solución: reemplazar strings vacíos/en blanco por NaN en CAMPOS_OBLIGATORIOS.

    Columnas afectadas: CarrierName, AircraftType, OriginCityName, DestCityName, Class
    """
    print("  [R04] Detectando campos obligatorios vacíos...")

    df = df.copy()
    total_reemplazados = 0

    for campo in CAMPOS_OBLIGATORIOS:
        if campo not in df.columns:
            print(f"         ⚠ Campo '{campo}' no encontrado, se omite.")
            continue

        mascara = df[campo].apply(_es_vacio)
        cantidad = mascara.sum()

        if cantidad > 0:
            df.loc[mascara, campo] = None
            print(f"         '{campo}': {cantidad:,} vacíos → NULL")
            total_reemplazados += cantidad

    print(f"         Total reemplazados: {total_reemplazados:,}")
    return df


# =============================================================
# FUNCIÓN PRINCIPAL: encadena las tres reglas
# =============================================================

def aplicar_reglas(engine) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lee stg_t100 desde staging, aplica R02, R03 y R04 en orden,
    y retorna (df_t100_limpio, df_resumen).

    df_resumen contiene el conteo de filas afectadas por regla,
    útil para logging en load.py.
    """
    print("\n" + "=" * 60)
    print("TRANSFORM — Reglas R02, R03, R04")
    print("=" * 60)

    print("\nLeyendo stg_t100 desde staging...")
    df = pd.read_sql("SELECT * FROM dbo.stg_t100", engine)
    print(f"  Filas leídas: {len(df):,}")

    print()
    df = aplicar_r02(df)
    df = aplicar_r03(df)
    df = aplicar_r04(df)

    resumen = pd.DataFrame([
        {
            "regla": "R02",
            "descripcion": "Normalización UniqueCarrierName",
            "filas_afectadas": len(df),
        },
        {
            "regla": "R03",
            "descripcion": "AirlineID 0/NULL → Desconocido",
            "filas_afectadas": int((df["AirlineID"] == AIRLINE_ID_DESCONOCIDO).sum()),
        },
        {
            "regla": "R04",
            "descripcion": "Campos obligatorios vacíos → NULL",
            "filas_afectadas": int(
                df[[c for c in CAMPOS_OBLIGATORIOS if c in df.columns]]
                .isna().any(axis=1).sum()
            ),
        },
    ])

    print(f"\n{'='*60}")
    print("TRANSFORM COMPLETADO")
    print(f"  Filas procesadas: {len(df):,}")
    print(f"{'='*60}\n")

    return df, resumen


# =============================================================
# EJECUCIÓN DIRECTA (para pruebas)
# =============================================================

if __name__ == "__main__":
    if not CONNECTION_STRING:
        print("ERROR: No se encontró DB_CONNECTION_STRING en el .env")
        exit(1)

    engine = create_engine(CONNECTION_STRING, fast_executemany=True)

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Conexión exitosa.")
    except Exception as e:
        print(f"ERROR de conexión: {e}")
        exit(1)

    df_limpio, df_resumen = aplicar_reglas(engine)

    print("Resumen por regla:")
    print(df_resumen.to_string(index=False))
    print("\nMuestra del DataFrame limpio:")
    cols = ["AirlineID", "UniqueCarrier", "UniqueCarrierName", "CarrierName", "AircraftType", "Class"]
    print(df_limpio[[c for c in cols if c in df_limpio.columns]].head(10))
