"""
transform_lookups.py
--------------------
Limpieza de las tablas de lookup cargadas en dw_staging_raw por extract.py.
Lee desde staging, aplica las reglas de calidad y retorna DataFrames limpios
listos para la fase de carga (load.py).

TABLAS TRATADAS:
    stg_unique_carriers  — códigos y nombres de carriers
    stg_carrier_history  — historial de carriers y fusiones
    carrier_group_compact — tabla consolidada que unifica stg_carrier_group
                            y stg_carrier_group_new en una sola referencia

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
    resultado["unique_carriers"]      = limpiar_unique_carriers(engine)
    resultado["carrier_history"]      = limpiar_carrier_history(engine)
    resultado["carrier_group_compact"] = construir_carrier_group_compact()

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

    print("Muestra unique_carriers limpio:")
    print(dfs["unique_carriers"].to_string(index=False))

    print("\nMuestra carrier_history limpio:")
    print(dfs["carrier_history"].to_string(index=False))

    print("\ncarrier_group_compact:")
    print(dfs["carrier_group_compact"].to_string(index=False))
