"""
transform_lookups.py
--------------------
Limpieza de las tablas de lookup cargadas en dw_staging_raw por extract.py.
Lee desde staging, aplica las reglas de calidad y retorna DataFrames limpios
listos para la fase de carga (load.py).

TABLAS TRATADAS:
    stg_unique_carriers       — códigos y nombres de carriers
    stg_carrier_history       — historial de carriers y fusiones
    carrier_group_compact     — tabla consolidada que unifica stg_carrier_group
                                y stg_carrier_group_new en una sola referencia
    stg_aircraft_config       — configuración de aeronaves (pasillo simple/doble, etc.)
    stg_aircraft_group        — grupo de aeronave (Jet, Prop, Hel)
    stg_aircraft_type         — tipo de aeronave (737, A320, etc.)
    stg_airline_ids           — IDs numéricos de aerolíneas (AirlineID)
    stg_airport_ids           — IDs numéricos de aeropuertos
    stg_airport_seq_ids       — IDs secuenciales de aeropuertos por período
    stg_airports              — códigos IATA de aeropuertos
    stg_city_market_ids       — IDs de mercados de ciudad (CityMarketID)
    stg_distance_group        — grupos de distancia de vuelo
    stg_months                — meses del año
    stg_quarters              — trimestres del año
    stg_service_class         — clase de servicio de vuelo
    stg_unique_carrier_entities — entidades de carrier únicas
    stg_world_area_codes      — códigos de área del mundo

REQUISITOS:
    pip install pandas sqlalchemy pyodbc python-dotenv

CONFIGURACIÓN:
    Archivo .env con DB_CONNECTION_STRING en la raíz del proyecto.

USO (como módulo):
    from transform_lookups import limpiar_lookups
    dfs = limpiar_lookups(engine)   # dict: nombre_tabla → DataFrame limpio

USO (directo):
    python transform_lookups.py
"""

import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =============================================================
# CONFIGURACIÓN
# =============================================================

ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(ENV_PATH)

CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# Carriers confirmados como inactivos (fusionados o cerrados)
CARRIERS_INACTIVOS = {"CO", "US", "FL", "VX", "CP", "PA", "EA", "KS"}

# =============================================================
# stg_unique_carriers
# =============================================================

def limpiar_unique_carriers(engine) -> pd.DataFrame:
    """
    Lee stg_unique_carriers desde staging y aplica las siguientes correcciones:

    1. STRIP + UPPER en Code: elimina espacios y normaliza mayúsculas
       (' AA' → 'AA', 'aa' → 'AA', 'BW ' → 'BW').
    2. Elimina filas con Code nulo o vacío tras la normalización.
    3. Elimina códigos con sufijo numérico histórico (AA(1), CO(1), US(1), PA(1)):
       son artefactos del sistema DOT ya tratados en R01 sobre stg_t100.
    4. Resuelve duplicados: para códigos repetidos con descripción vacía,
       imputa la descripción desde la otra ocurrencia no vacía. Luego deduplica
       conservando la primera aparición canónica.
    5. Agrega columna IsActive (True/False) para distinguir carriers vigentes
       de los fusionados o cerrados (CO, US, FL, VX, CP, PA, EA, KS).
    """
    print("  [stg_unique_carriers] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_unique_carriers",
        engine,
    )
    filas_originales = len(df)

    # 1. Normalizar Code: strip + upper
    df["Code"] = df["Code"].str.strip().str.upper()
    df["Description"] = df["Description"].str.strip()

    # 2. Eliminar filas con Code vacío o nulo
    df = df[df["Code"].notna() & (df["Code"] != "")]

    # 3. Eliminar códigos con sufijo numérico histórico (contienen paréntesis)
    mask_sufijo = df["Code"].str.contains(r"\(", regex=True, na=False)
    n_sufijos = mask_sufijo.sum()
    codigos_sufijo = sorted(df.loc[mask_sufijo, "Code"].unique())
    df = df[~mask_sufijo]

    # 4. Resolver duplicados
    #    Para cada código repetido: si alguna ocurrencia tiene descripción vacía,
    #    rellenarla con la descripción no vacía del mismo código.
    df["Description"] = df["Description"].replace("", pd.NA)

    mapa_desc = (
        df[df["Description"].notna()]
        .drop_duplicates(subset="Code", keep="first")
        .set_index("Code")["Description"]
        .to_dict()
    )
    mask_sin_desc = df["Description"].isna()
    df.loc[mask_sin_desc, "Description"] = df.loc[mask_sin_desc, "Code"].map(mapa_desc)

    n_antes_dedup = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_duplicados = n_antes_dedup - len(df)

    # 5. Flag IsActive
    df["IsActive"] = ~df["Code"].isin(CARRIERS_INACTIVOS)

    filas_finales = len(df)
    print(f"         Filas originales : {filas_originales:,}")
    print(f"         Sufijos eliminados: {n_sufijos:,}  {codigos_sufijo}")
    print(f"         Duplicados resueltos: {n_duplicados:,}")
    print(f"         Filas limpias    : {filas_finales:,}")
    print(f"         Carriers inactivos flaggeados: {(~df['IsActive']).sum():,}")
    return df


# =============================================================
# stg_carrier_history
# =============================================================

def limpiar_carrier_history(engine) -> pd.DataFrame:
    """
    Lee stg_carrier_history desde staging y aplica las siguientes correcciones:

    1. Elimina filas con código IATA con sufijo numérico (AA(1), PA(1)):
       artefactos históricos del DOT, ya tratados por R01 en stg_t100.
    2. Parsea StartDate y EndDate a datetime para poder operar sobre ellas.
    3. Corrige fechas invertidas (StartDate > EndDate): se intercambian los valores.
    4. Elimina filas con EndDate posterior al año actual: detecta fechas ficticias
       como 2099-12-31 usadas en registros duplicados.
    5. Elimina filas con AirlineID fuera del rango válido del BTS (10000–90000),
       excluyendo el 0 que se trata por separado en R03. Detecta IDs inventados
       como 99999 sin depender de un valor específico hardcodeado.
    6. Detecta y elimina asignaciones cruzadas inválidas de AirlineID:
       caso Delta (DL) con AirlineID 19386 que pertenece a American Airlines.
       La detección distingue este caso de las fusiones legítimas (CO→UA, US→AA, etc.)
       verificando si el carrier tiene sus propias entradas con EndDate, lo que
       indicaría que cesó operaciones y cedió su código (fusión válida).
       DL sigue activo con su propio AirlineID (19690), por lo que usar el de AA es inválido.

    Nota: AirlineID = 0 (EA, PA) NO se elimina aquí — ya está contemplado por R03.
    Nota: el solapamiento de FL/WN en 2011-2014 se conserva: es un período de
          transición documentado, no un error.
    """
    print("  [stg_carrier_history] Limpiando...")

    df = pd.read_sql(
        "SELECT AirlineID, UniqueCarrier, CarrierName, StartDate, EndDate "
        "FROM dw_staging_raw.dbo.stg_carrier_history",
        engine,
    )
    filas_originales = len(df)

    # 1. Eliminar filas con sufijo numérico en el código IATA
    mask_sufijo = df["UniqueCarrier"].str.contains(r"\(", regex=True, na=False)
    n_sufijos = int(mask_sufijo.sum())
    codigos_sufijo = sorted(df.loc[mask_sufijo, "UniqueCarrier"].unique())
    df = df[~mask_sufijo].copy()

    # 2. Parsear fechas
    df["StartDate"] = pd.to_datetime(df["StartDate"], errors="coerce")
    df["EndDate"]   = pd.to_datetime(df["EndDate"],   errors="coerce")

    # 3. Corregir fechas invertidas (StartDate > EndDate)
    mask_invertida = df["EndDate"].notna() & (df["StartDate"] > df["EndDate"])
    n_invertidas = int(mask_invertida.sum())
    carriers_invertidos = sorted(df.loc[mask_invertida, "UniqueCarrier"].unique())
    df.loc[mask_invertida, ["StartDate", "EndDate"]] = (
        df.loc[mask_invertida, ["EndDate", "StartDate"]].values
    )

    # 4. Eliminar EndDate absurda (año > año actual → EndDate ficticia)
    anio_actual = pd.Timestamp.now().year
    mask_futuro = df["EndDate"].notna() & (df["EndDate"].dt.year > anio_actual)
    n_futuro = int(mask_futuro.sum())
    carriers_futuro = sorted(df.loc[mask_futuro, "UniqueCarrier"].unique())
    df = df[~mask_futuro].copy()

    # 5. Eliminar AirlineID inexistente
    # AirlineID viene como string desde SQL → convertir a numérico antes de comparar
    df["AirlineID"] = pd.to_numeric(df["AirlineID"], errors="coerce").astype("Int64")
    ids_validos_min, ids_validos_max = 10000, 90000  # rango real de AirlineIDs del BTS
    mask_id_invalido = df["AirlineID"].notna() & (
        (df["AirlineID"] < ids_validos_min) | (df["AirlineID"] > ids_validos_max)
    ) & (df["AirlineID"] != 0)
    n_id_invalido = int(mask_id_invalido.sum())
    ids_invalidos = [int(x) for x in sorted(df.loc[mask_id_invalido, "AirlineID"].unique())]
    df = df[~mask_id_invalido].copy()

    # 6. Detectar asignaciones cruzadas inválidas de AirlineID
    #    Para cada carrier, su AirlineID "primario" es el de su entrada más antigua.
    primary_id = (
        df[df["AirlineID"].notna() & (df["AirlineID"] != 0)]
        .sort_values("StartDate")
        .drop_duplicates("UniqueCarrier", keep="first")
        .set_index("UniqueCarrier")["AirlineID"]
        .to_dict()
    )

    # Solo se considera "dueño único" el AirlineID que corresponde a UN solo carrier
    # como primario. Los AirlineIDs compartidos por varios carriers (ej: 20397 entre
    # PT, TZ y FL) se excluyen para evitar falsos positivos.
    id_a_carriers = {}
    for carrier, aid in primary_id.items():
        id_a_carriers.setdefault(aid, set()).add(carrier)
    primary_carrier = {
        aid: list(carriers)[0]
        for aid, carriers in id_a_carriers.items()
        if len(carriers) == 1
    }

    # Carriers que tienen al menos una EndDate en su AirlineID propio
    # (indican cierre o fusión: cedieron su código legítimamente)
    carriers_con_cierre = set(
        df[
            df["EndDate"].notna() &
            (df["AirlineID"] == df["UniqueCarrier"].map(primary_id))
        ]["UniqueCarrier"]
    )

    def _es_cruce_invalido(row):
        if pd.isna(row["AirlineID"]) or row["AirlineID"] == 0:
            return False
        owner = primary_carrier.get(row["AirlineID"])
        if owner is None or owner == row["UniqueCarrier"]:
            return False
        # AirlineID pertenece a otro carrier con dueño único → válido solo si
        # este carrier tiene EndDate en sus propias entradas (cesó o se fusionó)
        return row["UniqueCarrier"] not in carriers_con_cierre

    mask_cruce = df.apply(_es_cruce_invalido, axis=1)
    n_cruce = int(mask_cruce.sum())
    carriers_cruce = sorted(df.loc[mask_cruce, "UniqueCarrier"].unique())
    df = df[~mask_cruce].reset_index(drop=True)

    print(f"         Filas originales        : {filas_originales:,}")
    print(f"         Sufijos eliminados       : {n_sufijos:,}  {codigos_sufijo}")
    print(f"         Fechas invertidas corr.  : {n_invertidas:,}  {carriers_invertidos}")
    print(f"         EndDate ficticia elim.   : {n_futuro:,}  {carriers_futuro}")
    print(f"         AirlineID inexistente    : {n_id_invalido:,}  {ids_invalidos}")
    print(f"         Cruces inválidos elim.   : {n_cruce:,}  {carriers_cruce}")
    print(f"         Filas limpias            : {len(df):,}")
    return df


# =============================================================
# carrier_group_compact
# =============================================================

# Mapeo Code → Type(s), derivado del análisis cruzado entre stg_carrier_group,
# stg_carrier_group_new y los valores reales de CarrierGroup/CarrierGroupNew en t100.
# C aparece tanto como Major como National en t100 → dos filas.
# F se unifica como "Other" (equivalente al código 4 de la tabla vieja),
# ignorando que en t100 aparece mezclado con otros tipos por error de datos.
# G no tiene descripción en la tabla nueva ni vuelos documentados → "Unknown".
# El Code NULL/Legacy de la tabla vieja se omite: no es mapeable sin contexto adicional.
CARRIER_GROUP_MAPPING = [
    {"Code": "A", "Type": "Major",    "Description": "Large Network Carriers (>40B ASMs)"},
    {"Code": "B", "Type": "National", "Description": "Medium Network Carriers (10-40B ASMs)"},
    {"Code": "C", "Type": "Major",    "Description": "Low-Cost Carriers"},
    {"Code": "C", "Type": "National", "Description": "Low-Cost Carriers"},
    {"Code": "D", "Type": "National", "Description": "Ultra Low-Cost Carriers"},
    {"Code": "E", "Type": "Regional", "Description": "Regional/Feeder Carriers"},
    {"Code": "F", "Type": "Other",    "Description": "Other/Charter"},
    {"Code": "G", "Type": "Unknown",  "Description": "Unknown"},
]


def construir_carrier_group_compact() -> pd.DataFrame:
    """
    Consolida stg_carrier_group y stg_carrier_group_new en una única tabla
    de referencia sin necesidad de leer desde staging, dado que el mapeo
    requiere conocimiento de dominio y no puede derivarse automáticamente.

    Problema: ambas tablas usan esquemas distintos (texto vs. letras) sin
    tabla de equivalencia, y l_carrier_group mezcla además códigos numéricos
    redundantes con los textuales. En t100, CarrierGroup usa texto (Major,
    National, Regional, Other) y CarrierGroupNew usa letras (A-F), pero no
    existe un cruce oficial entre ambos sistemas.

    Solución: tabla compacta construida a partir del análisis de los valores
    reales en t100, con una fila por combinación (Code, Type) válida.
    Permite relaciones N:M (ej: C aparece como Major y como National).

    Columnas:
        Code        — código del sistema nuevo (A-G)
        Type        — clasificación del sistema viejo (Major/National/Regional/Other/Unknown)
        Description — descripción del sistema nuevo

    Nota: Code NULL/Legacy de la tabla vieja se omite. Los registros de t100
          con CarrierGroupNew nulo deben imputarse usando CarrierGroup como
          referencia en la etapa de transformación del t100.
    """
    df = pd.DataFrame(CARRIER_GROUP_MAPPING)
    print("  [carrier_group_compact] Construida desde mapeo de dominio.")
    print(f"         Filas: {len(df):,}  "
          f"(códigos únicos: {df['Code'].nunique():,}, "
          f"tipos únicos: {df['Type'].nunique():,})")
    return df


# =============================================================
# stg_aircraft_config
# =============================================================

def limpiar_aircraft_config(engine) -> pd.DataFrame:
    """
    Lee stg_aircraft_config y aplica las siguientes correcciones:

    1. STRIP + UPPER en Code: normaliza mayúsculas y elimina espacios.
    2. Elimina filas con Code nulo o vacío (entrada "Unknown" sin código válido).
    3. Deduplica por Code conservando la primera aparición canónica.
       Caso: TWIN aparece como "Twin aisle" y "Twin-aisle wide" → se conserva la primera.
    """
    print("  [stg_aircraft_config] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_aircraft_config",
        engine,
    )
    filas_orig = len(df)

    df["Code"] = df["Code"].str.strip().str.upper()
    df["Description"] = df["Description"].str.strip().replace("", pd.NA)

    n_nulos = int(df["Code"].isna().sum() + (df["Code"] == "").sum())
    df = df[df["Code"].notna() & (df["Code"] != "")]

    n_antes = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_dup = n_antes - len(df)

    print(f"         Filas originales : {filas_orig:,}")
    print(f"         Nulos elim. (Code): {n_nulos:,}")
    print(f"         Duplicados elim. : {n_dup:,}")
    print(f"         Filas limpias    : {len(df):,}")
    return df


# =============================================================
# stg_aircraft_group
# =============================================================

def limpiar_aircraft_group(engine) -> pd.DataFrame:
    """
    Lee stg_aircraft_group y aplica las siguientes correcciones:

    1. STRIP + UPPER en Code: normaliza "jet" → "JET" y elimina espacios.
    2. Elimina filas con Code nulo o vacío (entrada "Not reported" sin código).
    3. Deduplica por Code conservando la primera aparición canónica.
       Caso: JET y jet son el mismo código tras normalizar → se conserva uno.
    """
    print("  [stg_aircraft_group] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_aircraft_group",
        engine,
    )
    filas_orig = len(df)

    df["Code"] = df["Code"].str.strip().str.upper()
    df["Description"] = df["Description"].str.strip().replace("", pd.NA)

    n_nulos = int(df["Code"].isna().sum() + (df["Code"] == "").sum())
    df = df[df["Code"].notna() & (df["Code"] != "")]

    n_antes = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_dup = n_antes - len(df)

    print(f"         Filas originales  : {filas_orig:,}")
    print(f"         Nulos elim. (Code): {n_nulos:,}")
    print(f"         Dup. de case elim.: {n_dup:,}")
    print(f"         Filas limpias     : {len(df):,}")
    return df


# =============================================================
# stg_aircraft_type
# =============================================================

def limpiar_aircraft_type(engine) -> pd.DataFrame:
    """
    Lee stg_aircraft_type y aplica las siguientes correcciones:

    1. STRIP + UPPER en Code: normaliza mayúsculas y espacios.
    2. Elimina filas con Code nulo o vacío.
    3. Imputa descripciones vacías desde otra ocurrencia del mismo código.
       Caso: DC9 no tiene descripción y no existe otra entrada de DC9 → queda como NA.
    4. Deduplica por Code conservando la primera aparición canónica.
       Caso: 737 aparece como "Boeing 737 Classic" y "Boeing 737 (legacy code)"
       → se conserva "Boeing 737 Classic".
    """
    print("  [stg_aircraft_type] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_aircraft_type",
        engine,
    )
    filas_orig = len(df)

    df["Code"] = df["Code"].str.strip().str.upper()
    df["Description"] = df["Description"].str.strip().replace("", pd.NA)

    n_nulos = int(df["Code"].isna().sum() + (df["Code"] == "").sum())
    df = df[df["Code"].notna() & (df["Code"] != "")]

    # Imputar descripciones vacías desde otra ocurrencia del mismo código
    mapa_desc = (
        df[df["Description"].notna()]
        .drop_duplicates(subset="Code", keep="first")
        .set_index("Code")["Description"]
        .to_dict()
    )
    mask_sin_desc = df["Description"].isna()
    n_sin_desc = int(mask_sin_desc.sum())
    df.loc[mask_sin_desc, "Description"] = df.loc[mask_sin_desc, "Code"].map(mapa_desc)

    n_antes = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_dup = n_antes - len(df)

    print(f"         Filas originales  : {filas_orig:,}")
    print(f"         Nulos elim. (Code): {n_nulos:,}")
    print(f"         Sin desc. (NA)    : {n_sin_desc:,}")
    print(f"         Duplicados elim.  : {n_dup:,}")
    print(f"         Filas limpias     : {len(df):,}")
    return df


# =============================================================
# stg_airline_ids
# =============================================================

def limpiar_airline_ids(engine) -> pd.DataFrame:
    """
    Lee stg_airline_ids y aplica las siguientes correcciones:

    1. Convierte Code a numérico (Int64): asegura comparaciones correctas.
    2. Elimina filas con Code nulo.
    3. Imputa descripciones vacías desde otra ocurrencia del mismo código.
       Caso: 21171 (Sun Country) tiene una segunda entrada con descripción vacía.
    4. Deduplica por Code conservando la primera aparición canónica.
       Casos: 21171 (Sun Country / vacío), 20397 (Piedmont / AirTran),
       19977 (United / Continental post-fusión) → se conserva la primera.

    Nota: Code = 0 ("Unknown Carrier") se conserva como valor centinela.
    """
    print("  [stg_airline_ids] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_airline_ids",
        engine,
    )
    filas_orig = len(df)

    df["Code"] = pd.to_numeric(df["Code"], errors="coerce").astype("Int64")
    df["Description"] = df["Description"].str.strip().replace("", pd.NA)

    n_nulos = int(df["Code"].isna().sum())
    df = df[df["Code"].notna()]

    mapa_desc = (
        df[df["Description"].notna()]
        .drop_duplicates(subset="Code", keep="first")
        .set_index("Code")["Description"]
        .to_dict()
    )
    mask_sin_desc = df["Description"].isna()
    n_imputadas = int(mask_sin_desc.sum())
    df.loc[mask_sin_desc, "Description"] = df.loc[mask_sin_desc, "Code"].map(mapa_desc)

    n_antes = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_dup = n_antes - len(df)

    print(f"         Filas originales  : {filas_orig:,}")
    print(f"         Nulos elim. (Code): {n_nulos:,}")
    print(f"         Desc. imputadas   : {n_imputadas:,}")
    print(f"         Duplicados elim.  : {n_dup:,}")
    print(f"         Filas limpias     : {len(df):,}")
    return df


# =============================================================
# stg_airport_ids
# =============================================================

def limpiar_airport_ids(engine) -> pd.DataFrame:
    """
    Lee stg_airport_ids y aplica las siguientes correcciones:

    1. Convierte Code a numérico (Int64).
    2. Elimina filas con Code nulo.
    3. Elimina códigos con descripción nula y fuera del rango válido de AirportIDs
       del BTS (10000–19999). Detecta sentinelas inválidos como 99999 sin
       depender de un valor hardcodeado.
    4. Deduplica por Code conservando la primera aparición canónica.
       Caso: 11298 (O'Hare) aparece con dos nombres ligeramente distintos.

    Nota: Code = 0 ("Unknown Airport") se conserva como valor centinela.
    """
    print("  [stg_airport_ids] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_airport_ids",
        engine,
    )
    filas_orig = len(df)

    df["Code"] = pd.to_numeric(df["Code"], errors="coerce").astype("Int64")
    df["Description"] = df["Description"].str.strip().replace("", pd.NA)

    n_nulos = int(df["Code"].isna().sum())
    df = df[df["Code"].notna()]

    # AirportIDs válidos del BTS: 10000–19999. Códigos con descripción nula
    # y fuera de este rango son sentinelas inválidos (ej: 99999).
    mask_invalido = df["Description"].isna() & (
        (df["Code"] < 10000) | (df["Code"] > 19999)
    ) & (df["Code"] != 0)
    n_invalidos = int(mask_invalido.sum())
    codigos_invalidos = [int(x) for x in sorted(df.loc[mask_invalido, "Code"].unique())]
    df = df[~mask_invalido]

    n_antes = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_dup = n_antes - len(df)

    print(f"         Filas originales  : {filas_orig:,}")
    print(f"         Nulos elim. (Code): {n_nulos:,}")
    print(f"         Inválidos elim.   : {n_invalidos:,}  {codigos_invalidos}")
    print(f"         Duplicados elim.  : {n_dup:,}")
    print(f"         Filas limpias     : {len(df):,}")
    return df


# =============================================================
# stg_airport_seq_ids
# =============================================================

def limpiar_airport_seq_ids(engine) -> pd.DataFrame:
    """
    Lee stg_airport_seq_ids y aplica las siguientes correcciones:

    1. Convierte Code a numérico (Int64).
    2. Elimina filas con Code nulo.
    3. Elimina códigos cuyo formato no corresponde a un SeqID válido:
       los SeqIDs del BTS son de 7 dígitos (≥ 1.000.000). Códigos menores
       corresponden a AirportIDs cargados erróneamente en este campo
       (ej: 10135 que es un AirportID, no un SeqID).
    4. Elimina códigos con descripción nula que corresponden a sentinelas
       inválidos (ej: 9999999).
    5. Deduplica por Code conservando la primera aparición canónica.
    """
    print("  [stg_airport_seq_ids] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_airport_seq_ids",
        engine,
    )
    filas_orig = len(df)

    df["Code"] = pd.to_numeric(df["Code"], errors="coerce").astype("Int64")
    df["Description"] = df["Description"].str.strip().replace("", pd.NA)

    n_nulos = int(df["Code"].isna().sum())
    df = df[df["Code"].notna()]

    # SeqIDs válidos son de 7 dígitos (≥ 1.000.000)
    mask_formato = df["Code"] < 1_000_000
    n_formato = int(mask_formato.sum())
    codigos_formato = [int(x) for x in sorted(df.loc[mask_formato, "Code"].unique())]
    df = df[~mask_formato]

    # Sentinelas inválidos: descripción nula
    mask_sin_desc = df["Description"].isna()
    n_sin_desc = int(mask_sin_desc.sum())
    codigos_sin_desc = [int(x) for x in sorted(df.loc[mask_sin_desc, "Code"].unique())]
    df = df[~mask_sin_desc]

    n_antes = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_dup = n_antes - len(df)

    print(f"         Filas originales  : {filas_orig:,}")
    print(f"         Nulos elim. (Code): {n_nulos:,}")
    print(f"         Formato inv. elim.: {n_formato:,}  {codigos_formato}")
    print(f"         Sin desc. elim.   : {n_sin_desc:,}  {codigos_sin_desc}")
    print(f"         Duplicados elim.  : {n_dup:,}")
    print(f"         Filas limpias     : {len(df):,}")
    return df


# =============================================================
# stg_airports
# =============================================================

def limpiar_airports(engine) -> pd.DataFrame:
    """
    Lee stg_airports y aplica las siguientes correcciones:

    1. STRIP + UPPER en Code: normaliza mayúsculas y espacios.
    2. Elimina filas con Code nulo o vacío (entrada "Unknown Airport" sin código IATA).
    3. Imputa descripciones vacías desde otra ocurrencia del mismo código.
       Caso: DAL no tiene descripción y no existe otra entrada → queda como NA.
    4. Deduplica por Code conservando la primera aparición canónica.
       Caso: ORD aparece con dos nombres distintos → se conserva el primero.
    """
    print("  [stg_airports] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_airports",
        engine,
    )
    filas_orig = len(df)

    df["Code"] = df["Code"].str.strip().str.upper()
    df["Description"] = df["Description"].str.strip().replace("", pd.NA)

    n_nulos = int(df["Code"].isna().sum() + (df["Code"] == "").sum())
    df = df[df["Code"].notna() & (df["Code"] != "")]

    mapa_desc = (
        df[df["Description"].notna()]
        .drop_duplicates(subset="Code", keep="first")
        .set_index("Code")["Description"]
        .to_dict()
    )
    mask_sin_desc = df["Description"].isna()
    n_imputadas = int(mask_sin_desc.sum())
    df.loc[mask_sin_desc, "Description"] = df.loc[mask_sin_desc, "Code"].map(mapa_desc)

    n_antes = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_dup = n_antes - len(df)

    print(f"         Filas originales  : {filas_orig:,}")
    print(f"         Nulos elim. (Code): {n_nulos:,}")
    print(f"         Desc. imputadas   : {n_imputadas:,}")
    print(f"         Duplicados elim.  : {n_dup:,}")
    print(f"         Filas limpias     : {len(df):,}")
    return df


# =============================================================
# stg_city_market_ids
# =============================================================

def limpiar_city_market_ids(engine) -> pd.DataFrame:
    """
    Lee stg_city_market_ids y aplica las siguientes correcciones:

    1. Convierte Code a numérico (Int64).
    2. Elimina filas con Code nulo.
    3. Elimina códigos fuera del rango válido de CityMarketIDs del BTS (30000–39999).
       Detecta AirportIDs cargados en este campo (rango 10000–19999, ej: 10135, 11432)
       y sentinelas inválidos (ej: 99000) sin depender de valores hardcodeados.
    4. Deduplica por Code conservando la primera aparición canónica.
       Caso: 34819 aparece como "New York, NY" y "Newark, NJ" → se conserva el primero.
    """
    print("  [stg_city_market_ids] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_city_market_ids",
        engine,
    )
    filas_orig = len(df)

    df["Code"] = pd.to_numeric(df["Code"], errors="coerce").astype("Int64")
    df["Description"] = df["Description"].str.strip().replace("", pd.NA)

    n_nulos = int(df["Code"].isna().sum())
    df = df[df["Code"].notna()]

    ID_MIN, ID_MAX = 30000, 39999
    mask_invalido = (df["Code"] < ID_MIN) | (df["Code"] > ID_MAX)
    n_invalidos = int(mask_invalido.sum())
    codigos_invalidos = [int(x) for x in sorted(df.loc[mask_invalido, "Code"].unique())]
    df = df[~mask_invalido]

    n_antes = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_dup = n_antes - len(df)

    print(f"         Filas originales   : {filas_orig:,}")
    print(f"         Nulos elim. (Code) : {n_nulos:,}")
    print(f"         IDs fuera de rango : {n_invalidos:,}  {codigos_invalidos}")
    print(f"         Duplicados elim.   : {n_dup:,}")
    print(f"         Filas limpias      : {len(df):,}")
    return df


# =============================================================
# stg_distance_group
# =============================================================

def limpiar_distance_group(engine) -> pd.DataFrame:
    """
    Lee stg_distance_group y aplica las siguientes correcciones:

    1. Convierte Code a numérico (Int64).
    2. Elimina filas con Code nulo o vacío ("Unknown Distance Group" sin código).
    3. Deduplica por Code conservando la primera aparición canónica.
       Caso: código 1 aparece como "Under 500 Miles" y "Less than 500 Miles"
       → se conserva la primera descripción.

    Nota: Code = 0 ("Distance not reported") se conserva como valor centinela.
    """
    print("  [stg_distance_group] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_distance_group",
        engine,
    )
    filas_orig = len(df)

    df["Code"] = pd.to_numeric(df["Code"], errors="coerce").astype("Int64")
    df["Description"] = df["Description"].str.strip().replace("", pd.NA)

    n_nulos = int(df["Code"].isna().sum())
    df = df[df["Code"].notna()]

    n_antes = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_dup = n_antes - len(df)

    print(f"         Filas originales  : {filas_orig:,}")
    print(f"         Nulos elim. (Code): {n_nulos:,}")
    print(f"         Duplicados elim.  : {n_dup:,}")
    print(f"         Filas limpias     : {len(df):,}")
    return df


# =============================================================
# stg_months
# =============================================================

def limpiar_months(engine) -> pd.DataFrame:
    """
    Lee stg_months y aplica las siguientes correcciones:

    1. Detecta y elimina códigos no numéricos (JAN, FEB, etc.): el esquema
       canónico del BTS usa enteros 0–12. Las variantes textuales son artefactos
       de una carga mixta y duplican meses ya presentes con código numérico.
    2. Convierte Code a numérico (Int64).
    3. Deduplica por Code conservando la primera aparición canónica.

    Nota: Code = 0 ("Month not reported") se conserva como valor centinela.
    """
    print("  [stg_months] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_months",
        engine,
    )
    filas_orig = len(df)

    df["Description"] = df["Description"].str.strip().replace("", pd.NA)

    # Detectar códigos no numéricos (JAN, FEB, etc.)
    df["_code_num"] = pd.to_numeric(df["Code"], errors="coerce")
    mask_texto = df["_code_num"].isna() & df["Code"].notna()
    n_texto = int(mask_texto.sum())
    codigos_texto = sorted(df.loc[mask_texto, "Code"].dropna().unique())
    df = df[~mask_texto].copy()
    df["Code"] = df["_code_num"].astype("Int64")
    df = df.drop(columns=["_code_num"])

    n_nulos = int(df["Code"].isna().sum())
    df = df[df["Code"].notna()]

    n_antes = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_dup = n_antes - len(df)

    print(f"         Filas originales   : {filas_orig:,}")
    print(f"         Códigos texto elim.: {n_texto:,}  {codigos_texto}")
    print(f"         Nulos elim. (Code) : {n_nulos:,}")
    print(f"         Duplicados elim.   : {n_dup:,}")
    print(f"         Filas limpias      : {len(df):,}")
    return df


# =============================================================
# stg_quarters
# =============================================================

def limpiar_quarters(engine) -> pd.DataFrame:
    """
    Lee stg_quarters y aplica las siguientes correcciones:

    1. Convierte Code a numérico (Int64).
    2. Elimina filas con Code nulo.
    3. Deduplica por Code conservando la primera aparición canónica.
       Caso: códigos 1–4 aparecen dos veces: con descripciones completas
       ("January-March") y con alias cortos ("Q1"). Se conservan las primeras.

    Nota: Code = 0 ("Quarter not reported") se conserva como valor centinela.
    """
    print("  [stg_quarters] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_quarters",
        engine,
    )
    filas_orig = len(df)

    df["Code"] = pd.to_numeric(df["Code"], errors="coerce").astype("Int64")
    df["Description"] = df["Description"].str.strip().replace("", pd.NA)

    n_nulos = int(df["Code"].isna().sum())
    df = df[df["Code"].notna()]

    n_antes = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_dup = n_antes - len(df)

    print(f"         Filas originales  : {filas_orig:,}")
    print(f"         Nulos elim. (Code): {n_nulos:,}")
    print(f"         Duplicados elim.  : {n_dup:,}")
    print(f"         Filas limpias     : {len(df):,}")
    return df


# =============================================================
# stg_service_class
# =============================================================

def limpiar_service_class(engine) -> pd.DataFrame:
    """
    Lee stg_service_class y aplica las siguientes correcciones:

    1. STRIP en Code.
    2. Elimina filas con Code nulo o vacío (entrada "Unknown Service Class").
    3. Elimina códigos numéricos (ej: "1"): el esquema del BTS usa exclusivamente
       letras (F, G, L, P, Q, R, Z). Los códigos numéricos son artefactos de
       una carga mixta con otro esquema de clasificación.
    4. Deduplica por Code conservando la primera aparición canónica.
       Casos: F y L aparecen con descripciones cortas y largas → se conservan
       las más completas (primera aparición).
    """
    print("  [stg_service_class] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_service_class",
        engine,
    )
    filas_orig = len(df)

    df["Code"] = df["Code"].str.strip()
    df["Description"] = df["Description"].str.strip().replace("", pd.NA)

    n_nulos = int(df["Code"].isna().sum() + (df["Code"] == "").sum())
    df = df[df["Code"].notna() & (df["Code"] != "")]

    mask_numerico = df["Code"].str.isnumeric()
    n_numericos = int(mask_numerico.sum())
    codigos_num = sorted(df.loc[mask_numerico, "Code"].unique())
    df = df[~mask_numerico]

    n_antes = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_dup = n_antes - len(df)

    print(f"         Filas originales  : {filas_orig:,}")
    print(f"         Nulos elim. (Code): {n_nulos:,}")
    print(f"         Numéricos elim.   : {n_numericos:,}  {codigos_num}")
    print(f"         Duplicados elim.  : {n_dup:,}")
    print(f"         Filas limpias     : {len(df):,}")
    return df


# =============================================================
# stg_unique_carrier_entities
# =============================================================

def limpiar_unique_carrier_entities(engine) -> pd.DataFrame:
    """
    Lee stg_unique_carrier_entities y aplica las siguientes correcciones:

    1. STRIP + UPPER en Code: normaliza mayúsculas y espacios.
    2. Elimina filas con Code nulo o vacío.
    3. Imputa descripciones vacías desde otra ocurrencia del mismo código.
       Caso: XX no tiene descripción en ninguna ocurrencia → queda como NA.
    4. Deduplica por Code conservando la primera aparición canónica.
       Caso: AA aparece con dos variantes de nombre → se conserva la primera.
    """
    print("  [stg_unique_carrier_entities] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_unique_carrier_entities",
        engine,
    )
    filas_orig = len(df)

    df["Code"] = df["Code"].str.strip().str.upper()
    df["Description"] = df["Description"].str.strip().replace("", pd.NA)

    n_nulos = int(df["Code"].isna().sum() + (df["Code"] == "").sum())
    df = df[df["Code"].notna() & (df["Code"] != "")]

    mapa_desc = (
        df[df["Description"].notna()]
        .drop_duplicates(subset="Code", keep="first")
        .set_index("Code")["Description"]
        .to_dict()
    )
    mask_sin_desc = df["Description"].isna()
    n_imputadas = int(mask_sin_desc.sum())
    df.loc[mask_sin_desc, "Description"] = df.loc[mask_sin_desc, "Code"].map(mapa_desc)

    n_antes = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_dup = n_antes - len(df)

    print(f"         Filas originales  : {filas_orig:,}")
    print(f"         Nulos elim. (Code): {n_nulos:,}")
    print(f"         Desc. imputadas   : {n_imputadas:,}")
    print(f"         Duplicados elim.  : {n_dup:,}")
    print(f"         Filas limpias     : {len(df):,}")
    return df


# =============================================================
# stg_world_area_codes
# =============================================================

def limpiar_world_area_codes(engine) -> pd.DataFrame:
    """
    Lee stg_world_area_codes y aplica las siguientes correcciones:

    1. Convierte Code a numérico (Int64).
    2. Elimina filas con Code nulo.
    3. Elimina Code = 0: no tiene descripción asignada y no es un centinela
       estándar del BTS para esta tabla (a diferencia de meses o trimestres).
    4. Deduplica por Code conservando la primera aparición canónica.
       Caso: código 9 ("Pacific") aparece también como "Pacific Region"
       → se conserva la primera descripción.
    """
    print("  [stg_world_area_codes] Limpiando...")

    df = pd.read_sql(
        "SELECT Code, Description FROM dw_staging_raw.dbo.stg_world_area_codes",
        engine,
    )
    filas_orig = len(df)

    df["Code"] = pd.to_numeric(df["Code"], errors="coerce").astype("Int64")
    df["Description"] = df["Description"].str.strip().replace("", pd.NA)

    n_nulos = int(df["Code"].isna().sum())
    df = df[df["Code"].notna()]

    mask_cero = df["Code"] == 0
    n_cero = int(mask_cero.sum())
    df = df[~mask_cero]

    n_antes = len(df)
    df = df.drop_duplicates(subset="Code", keep="first").reset_index(drop=True)
    n_dup = n_antes - len(df)

    print(f"         Filas originales  : {filas_orig:,}")
    print(f"         Nulos elim. (Code): {n_nulos:,}")
    print(f"         Código 0 elim.    : {n_cero:,}")
    print(f"         Duplicados elim.  : {n_dup:,}")
    print(f"         Filas limpias     : {len(df):,}")
    return df


# =============================================================
# FUNCIÓN PRINCIPAL
# =============================================================

def limpiar_lookups(engine) -> dict[str, pd.DataFrame]:
    """
    Orquesta la limpieza de todas las tablas lookup.
    Retorna un dict {nombre_tabla: DataFrame_limpio}.
    """
    print("\n" + "=" * 60)
    print("TRANSFORM LOOKUPS")
    print("=" * 60)

    resultado = {}
    resultado["unique_carriers"]           = limpiar_unique_carriers(engine)
    resultado["carrier_history"]           = limpiar_carrier_history(engine)
    resultado["carrier_group_compact"]     = construir_carrier_group_compact()
    resultado["aircraft_config"]           = limpiar_aircraft_config(engine)
    resultado["aircraft_group"]            = limpiar_aircraft_group(engine)
    resultado["aircraft_type"]             = limpiar_aircraft_type(engine)
    resultado["airline_ids"]               = limpiar_airline_ids(engine)
    resultado["airport_ids"]               = limpiar_airport_ids(engine)
    resultado["airport_seq_ids"]           = limpiar_airport_seq_ids(engine)
    resultado["airports"]                  = limpiar_airports(engine)
    resultado["city_market_ids"]           = limpiar_city_market_ids(engine)
    resultado["distance_group"]            = limpiar_distance_group(engine)
    resultado["months"]                    = limpiar_months(engine)
    resultado["quarters"]                  = limpiar_quarters(engine)
    resultado["service_class"]             = limpiar_service_class(engine)
    resultado["unique_carrier_entities"]   = limpiar_unique_carrier_entities(engine)
    resultado["world_area_codes"]          = limpiar_world_area_codes(engine)

    print(f"\n{'='*60}")
    print("TRANSFORM LOOKUPS COMPLETADO")
    print(f"{'='*60}\n")

    return resultado


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

    dfs = limpiar_lookups(engine)

    for nombre, df in dfs.items():
        print(f"\nMuestra {nombre}:")
        print(df.to_string(index=False))
