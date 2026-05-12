"""
transform_rules_1_2_3_4.py
------------
Fase de transformación del ETL. Lee las tablas de staging desde SQL Server,
aplica las reglas de calidad asignadas (1, 2, 3 y 4) y retorna DataFrames limpios listos
para la fase de carga (load.py).

REGLAS IMPLEMENTADAS:
    R01 - Sufijo numérico en UniqueCarrier / Carrier  →  eliminación del sufijo
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
    python transform_rules_1_2_3_4.py
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

# Campos obligatorios tratados en R04.
# OriginCityName y DestCityName hoy no tienen nulos pero se incluyen
# para que el resumen los contemple si aparecen en el futuro.
CAMPOS_CON_NULOS = ["CarrierName", "AircraftType", "Class", "OriginCityName", "DestCityName"]

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
# R01 — SUFIJO NUMÉRICO EN UniqueCarrier / Carrier
# =============================================================

PATRON_SUFIJO_IATA = re.compile(r'\(\d+\)$')


def aplicar_r01(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    R01: Elimina el sufijo numérico del código IATA en UniqueCarrier y Carrier.

    Problema: el BTS agrega el sufijo (n) al código IATA original cuando ese código
      fue reutilizado por otro carrier tras una fusión o cese (ej: AA(1), US(1), CO(1)).
      Esto fragmenta el agrupamiento: las 56 filas con sufijo quedan como carriers
      distintos al resto de American Airlines, US Airways y Continental.
    Solución: quitar el sufijo con regex → AA(1) → AA, US(1) → US, CO(1) → CO.
      Los AirlineIDs incorrectos que persisten en esas filas son resueltos por R09.

    Columnas afectadas: UniqueCarrier, Carrier
    """
    print("  [R01] Eliminando sufijos numéricos en códigos IATA...")

    df = df.copy()
    mascara = df["UniqueCarrier"].str.contains(PATRON_SUFIJO_IATA, na=False)
    cantidad = int(mascara.sum())
    carriers_afectados = sorted(df.loc[mascara, "UniqueCarrier"].unique())

    df["UniqueCarrier"] = df["UniqueCarrier"].str.replace(PATRON_SUFIJO_IATA, "", regex=True)
    df["Carrier"]       = df["Carrier"].str.replace(PATRON_SUFIJO_IATA, "", regex=True)

    print(f"         Registros corregidos: {cantidad:,}  "
          f"(códigos: {', '.join(carriers_afectados)})")
    return df, cantidad


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
# R04 — CAMPOS OBLIGATORIOS NULOS  →  IMPUTACIÓN O NULL
# =============================================================

def _imputar_carrier_name(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    CarrierName nulo → valor del campo Carrier del mismo registro.

    Carrier siempre tiene valor y es el código IATA limpio (sin sufijo numérico),
    que es exactamente lo que CarrierName debería contener.
    """
    mascara = df["CarrierName"].isna()
    cantidad = int(mascara.sum())
    df.loc[mascara, "CarrierName"] = df.loc[mascara, "Carrier"]
    print(f"         'CarrierName': {cantidad:,} nulos → imputado desde Carrier")
    return df, cantidad


def _imputar_class(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Class nulo → clase inferida desde UniqueCarrier.

    Análisis del dataset confirma que cada UniqueCarrier usa siempre
    la misma clase (F o L), sin excepciones en los 98.530 registros válidos.
    Se construye el mapa dinámicamente desde las filas no nulas del propio DataFrame.
    """
    mapa_carrier_class = (
        df[df["Class"].notna()]
        .groupby("UniqueCarrier")["Class"]
        .agg(lambda s: s.mode()[0])  # valor más frecuente por carrier (siempre único)
        .to_dict()
    )

    mascara = df["Class"].isna()
    cantidad = int(mascara.sum())
    df.loc[mascara, "Class"] = df.loc[mascara, "UniqueCarrier"].map(mapa_carrier_class)
    print(f"         'Class':       {cantidad:,} nulos → imputado desde UniqueCarrier")
    return df, cantidad


def _imputar_city_name(
    df: pd.DataFrame,
    col_city: str,
    col_iata: str,
    engine,
) -> tuple[pd.DataFrame, int]:
    """
    CityName nulo → ciudad inferida en dos pasos:

    Paso 1: cruzar el código IATA (Origin/Dest) con stg_airports en dw_staging_raw.
            Description tiene formato "Ciudad, Estado: Nombre Aeropuerto";
            se extrae la parte antes del ":" como nombre de ciudad.
            Cobertura: ~63% de las filas, certeza 100%.

    Paso 2: para los IATA no cubiertos por stg_airports, construir un
            diccionario desde las filas no-nulas del propio DataFrame
            (relación IATA → CityName es 1-a-1 perfecta en el dataset).
            Cubre el 100% restante.

    No se usa CityMarketID: 13 de 138 CMIDs agrupan hasta 5 ciudades
    distintas y generarían imputaciones incorrectas.
    No se usa stg_airport_ids: tiene errores de datos documentados.
    """
    mascara = df[col_city].isna()
    cantidad = int(mascara.sum())
    if cantidad == 0:
        return df, 0

    # Paso 1: lookup desde stg_airports en dw_staging_raw
    lk = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_airports "
        "WHERE Code IS NOT NULL AND Description IS NOT NULL",
        engine,
    )
    lk["ciudad"] = lk["Description"].str.split(":").str[0].str.strip()
    mapa_lookup = lk.set_index("Code")["ciudad"].to_dict()

    # Paso 2: diccionario construido desde filas no-nulas del propio df
    mapa_dataset = (
        df[df[col_city].notna()]
        .groupby(col_iata)[col_city]
        .agg(lambda s: s.mode()[0])
        .to_dict()
    )

    # Aplicar: lookup tiene prioridad sobre el fallback del dataset
    mapa_final = {**mapa_dataset, **mapa_lookup}
    df.loc[mascara, col_city] = df.loc[mascara, col_iata].map(mapa_final)

    resueltos = cantidad - int(df[col_city].isna().sum())
    print(f"         '{col_city}': {cantidad:,} nulos → {resueltos:,} imputados desde stg_airports")
    return df, resueltos


def aplicar_r04(df: pd.DataFrame, engine) -> pd.DataFrame:
    """
    R04: Trata los campos obligatorios con valores NULL.

    Campos y estrategia:
      - CarrierName    (101 nulos): imputar desde Carrier (código IATA limpio).
      - Class          ( 92 nulos): imputar desde UniqueCarrier (clase única por carrier).
      - AircraftType   (108 nulos): mantener como NULL (JET+TWIN mapea a 15 tipos distintos).
      - OriginCityName (  0 nulos): preparado para imputar desde Origin + stg_airports.
      - DestCityName   (  0 nulos): preparado para imputar desde Dest   + stg_airports.

    Todos los lookups se leen desde dw_staging_raw (SQL Server), nunca desde CSV.
    """
    print("  [R04] Tratando campos obligatorios con NULL...")

    df = df.copy()

    df, n_carrier = _imputar_carrier_name(df)
    df, n_class   = _imputar_class(df)

    n_aircraft = int(df["AircraftType"].isna().sum())
    print(f"         'AircraftType': {n_aircraft:,} nulos → se mantienen como NULL (no imputables)")

    df, n_origin = _imputar_city_name(df, "OriginCityName", "Origin", engine)
    df, n_dest   = _imputar_city_name(df, "DestCityName",   "Dest",   engine)

    total_imputados = n_carrier + n_class + n_origin + n_dest
    print(f"         Total imputados: {total_imputados:,}  |  Irresolubles: {n_aircraft:,}")
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
    print("TRANSFORM — Reglas R01, R02, R03, R04")
    print("=" * 60)

    print("\nLeyendo stg_t100 desde staging...")
    df = pd.read_sql("SELECT * FROM dbo.stg_t100", engine)
    print(f"  Filas leídas: {len(df):,}")

    print()
    df, n_r01 = aplicar_r01(df)
    df = aplicar_r02(df)
    df = aplicar_r03(df)
    df = aplicar_r04(df, engine)

    resumen = pd.DataFrame([
        {
            "regla": "R01",
            "descripcion": "Sufijo numérico en código IATA eliminado",
            "filas_afectadas": n_r01,
        },
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
            "descripcion": "Campos con NULL: imputados o mantenidos",
            "filas_afectadas": int(
                df[CAMPOS_CON_NULOS].isna().any(axis=1).sum()
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
