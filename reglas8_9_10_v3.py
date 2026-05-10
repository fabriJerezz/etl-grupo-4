"""
03_transform_staging.py
-----------------------
Lee stg_t100 desde SQL Server, aplica tres reglas de calidad y transformación
usando pandas en memoria, y deja el DataFrame limpio listo para cargarse al DW.

REGLAS APLICADAS
----------------
  Regla 8  – AirTime / RampTime imposibles  : valores anómalos → NaN (la fila se conserva)
  Regla 9  – Código IATA reutilizado        : detección dinámica → AirlineID estandarizado
                                              + columna flag is_merged_carrier
  Regla 10 – Filas duplicadas exactas       : se elimina toda ocurrencia excepto la primera

REQUISITOS
----------
    pip install pandas sqlalchemy pyodbc python-dotenv

CONFIGURACIÓN
-------------
    Crear un archivo .env en la raíz del proyecto con:
        DB_CONNECTION_STRING=mssql+pyodbc://...

USO
---
    python transform.py
"""

import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# ================================================================
# CONFIGURACIÓN GLOBAL
# ================================================================

# Carga las variables de entorno desde el .env ubicado junto al script
ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(ENV_PATH)

# Cadena de conexión leída del .env (nunca hardcodeada en el código)
CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# Tabla de origen en la base de datos de staging
STG_TABLE = "stg_t100"

# ── Umbrales de validez para Regla 8 ──────────────────────────
# Cualquier valor fuera de [MIN, MAX] se considera físicamente imposible.
# 9 000 min ≈ 150 horas → ningún vuelo comercial supera ese valor.
# 0 o negativo tampoco tiene sentido operativo.
AIRTIME_MIN  =    1   # minutos mínimos razonables para AirTime
AIRTIME_MAX  = 9000   # minutos máximos razonables para AirTime
RAMPTIME_MIN =    1   # minutos mínimos razonables para RampTime
RAMPTIME_MAX = 9000   # minutos máximos razonables para RampTime


# ================================================================
# UTILIDADES DE CONSOLA
# ================================================================

def banner(titulo: str) -> None:
    """Imprime un encabezado visual para separar secciones en el log."""
    linea = "=" * 65
    print(f"\n{linea}")
    print(f"  {titulo}")
    print(linea)


def separador() -> None:
    """Línea fina de separación entre sub-pasos."""
    print(f"    {'─' * 59}")


def log_resumen_regla(regla: str, antes: int, despues: int, detalle: str = "") -> None:
    """
    Imprime el resumen de impacto al final de cada regla:
      - filas antes y después de aplicarla
      - delta absoluto y porcentual
      - detalle adicional opcional
    """
    delta = antes - despues
    pct   = (delta / antes * 100) if antes else 0.0
    print(f"\n  ┌─ RESULTADO {regla}")
    print(f"  │  Filas al entrar  : {antes:>10,}")
    print(f"  │  Filas al salir   : {despues:>10,}")
    print(f"  │  Filas afectadas  : {delta:>10,}  ({pct:.2f} % del total)")
    if detalle:
        print(f"  │  {detalle}")
    print(f"  └{'─' * 62}")


# ================================================================
# REGLA 8 – AirTime / RampTime con valores imposibles
# ================================================================

def regla_8_tiempos_imposibles(df: pd.DataFrame) -> pd.DataFrame:
    """
    PROPÓSITO
    ---------
    Neutraliza valores de AirTime y RampTime que son físicamente imposibles
    convirtiéndolos a NaN.  NO se elimina la fila completa porque el resto
    de los campos del vuelo (origen, destino, pasajeros, etc.) siguen siendo
    datos válidos y útiles para el DW.

    CRITERIO DE ANOMALÍA (para cada columna)
    -----------------------------------------
      • valor < 1      → cero o negativo; sin sentido operativo
      • valor > 9 000  → más de 150 h; ningún segmento comercial lo supera

    COLUMNAS TRATADAS
    -----------------
      AirTime  : tiempo de vuelo en minutos (rueda arriba → rueda abajo)
      RampTime : tiempo de puerta a puerta incluyendo rodaje
    """
    banner("REGLA 8 – Tiempos de vuelo imposibles")
    print(f"\n  Evaluando {df.shape[0]:,} registros...")
    print(f"  Umbrales aplicados:")
    print(f"    AirTime  → válido si entre {AIRTIME_MIN} y {AIRTIME_MAX:,} minutos")
    print(f"    RampTime → válido si entre {RAMPTIME_MIN} y {RAMPTIME_MAX:,} minutos")

    # Guardamos el conteo inicial; en esta regla no se eliminan filas,
    # pero lo registramos para mantener el patrón uniforme de log.
    antes = df.shape[0]

    for col, col_min, col_max in [
        ("AirTime",  AIRTIME_MIN,  AIRTIME_MAX),
        ("RampTime", RAMPTIME_MIN, RAMPTIME_MAX),
    ]:
        print(f"\n  {'─'*55}")
        print(f"  Columna: {col}")

        # ── Verificar que la columna existe en el DataFrame ──────
        if col not in df.columns:
            print(f"  ⚠  '{col}' no encontrada en el DataFrame – se omite.")
            continue

        # ── Conteo base antes de tocar la columna ────────────────
        # Diferenciamos NaN ya existentes (vienen del STG) de los que
        # generaremos nosotros, para no confundirlos en el reporte.
        nan_previos  = df[col].isna().sum()
        con_valor    = df[col].notna().sum()  # registros que sí tienen dato

        print(f"    Registros con valor (no NaN)          : {con_valor:>8,}")
        print(f"    NaN preexistentes (venían del STG)    : {nan_previos:>8,}")

        # ── Construir máscara de valores anómalos ────────────────
        # Solo evaluamos celdas con valor (notna) para no recontabilizar
        # NaN que ya existían antes de esta regla.
        mascara_negativos_cero = df[col].notna() & (df[col] < col_min)
        mascara_excesivos      = df[col].notna() & (df[col] > col_max)
        mascara_anomala        = mascara_negativos_cero | mascara_excesivos

        n_negativos_cero = mascara_negativos_cero.sum()
        n_excesivos      = mascara_excesivos.sum()
        n_anomalos       = mascara_anomala.sum()

        print(f"    Anómalos: negativos o cero (< {col_min})    : {n_negativos_cero:>8,}")
        print(f"    Anómalos: excesivos (> {col_max:,})         : {n_excesivos:>8,}")
        print(f"    Total anómalos → se convierten a NaN  : {n_anomalos:>8,}")

        # ── Aplicar corrección ────────────────────────────────────
        # Se reemplaza el valor anómalo por NaN.
        # La fila completa se conserva; solo esa celda queda nula.
        df.loc[mascara_anomala, col] = np.nan

        # ── Estado de la columna tras la corrección ───────────────
        nan_post   = df[col].isna().sum()
        validos    = df[col].notna().sum()
        print(f"    Registros válidos que permanecen      : {validos:>8,}")
        print(f"    NaN totales en la columna (post-regla): {nan_post:>8,}  "
              f"(prev: {nan_previos:,} + nuevos: {n_anomalos:,})")

    log_resumen_regla(
        "Regla 8",
        antes,
        df.shape[0],
        "La cantidad de filas no cambia: solo se nullifican celdas anómalas.",
    )
    return df


# ================================================================
# REGLA 9 – Código IATA reutilizado (detección dinámica)
# ================================================================

def _detectar_carriers_con_multiples_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    FUNCIÓN AUXILIAR – análisis estadístico previo a la corrección.

    QUÉ HACE
    --------
    Agrupa el DataFrame por (UniqueCarrier, AirlineID) para construir
    una tabla de frecuencias: cuántos registros y cuántos pasajeros
    tiene cada combinación código-ID.

    Luego filtra los UniqueCarrier que aparecen con MÁS DE UN AirlineID
    (indicio de reutilización del código tras una fusión o adquisición).

    Para cada uno de esos carriers anómalos determina el AirlineID
    DOMINANTE usando el siguiente criterio de desempate:
        1º) mayor cantidad de registros   → carrier con mayor presencia histórica
        2º) mayor suma de pasajeros       → desempate si los registros son iguales

    RETORNA
    -------
    DataFrame con una fila por carrier anómalo y las columnas:
        UniqueCarrier       – código IATA del carrier
        ids_distintos       – cuántos AirlineID distintos tiene ese código
        id_dominante        – el AirlineID que se usará como valor canónico
        conteo_dominante    – registros que respaldan al ID dominante
        pasajeros_dominante – pasajeros transportados bajo el ID dominante
    """
    # ── Paso A: tabla de frecuencias (UniqueCarrier × AirlineID) ─
    # Cuenta registros y suma pasajeros por cada combinación única.
    # Esto nos dice "cuántas veces aparece cada ID bajo cada código IATA".
    conteos = (
        df.groupby(["UniqueCarrier", "AirlineID"], as_index=False)
        .agg(
            n_registros=("AirlineID", "count"),
            n_pasajeros=("Passengers", "sum"),
        )
    )

    # ── Paso B: identificar carriers con más de un AirlineID ─────
    # nunique() nos dice cuántos IDs distintos tiene cada código IATA.
    # Si es > 1, ese código fue reutilizado en algún momento.
    ids_por_carrier   = conteos.groupby("UniqueCarrier")["AirlineID"].nunique()
    carriers_anomalos = ids_por_carrier[ids_por_carrier > 1].index

    # Sin anomalías → retornar DataFrame vacío para que la llamadora
    # pueda detectar el caso sin romper el flujo normal.
    if len(carriers_anomalos) == 0:
        return pd.DataFrame(
            columns=[
                "UniqueCarrier", "ids_distintos", "id_dominante",
                "conteo_dominante", "pasajeros_dominante",
            ]
        )

    # ── Paso C: hallar el AirlineID dominante por carrier ────────
    # Filtramos solo los carriers anómalos y ordenamos descendentemente
    # por n_registros; en caso de empate exacto, por n_pasajeros.
    # groupby().first() después del sort toma la fila de mayor peso.
    subset = conteos[conteos["UniqueCarrier"].isin(carriers_anomalos)].copy()

    subset = subset.sort_values(
        by=["UniqueCarrier", "n_registros", "n_pasajeros"],
        ascending=[True, False, False],
    )

    dominante = subset.groupby("UniqueCarrier").first().reset_index()
    dominante = dominante.rename(columns={
        "AirlineID":   "id_dominante",
        "n_registros": "conteo_dominante",
        "n_pasajeros": "pasajeros_dominante",
    })

    # Adjuntamos cuántos IDs distintos tiene cada carrier anómalo
    dominante["ids_distintos"] = dominante["UniqueCarrier"].map(ids_por_carrier)

    return dominante[[
        "UniqueCarrier", "ids_distintos", "id_dominante",
        "conteo_dominante", "pasajeros_dominante",
    ]]


def regla_9_iata_reutilizado(df: pd.DataFrame) -> pd.DataFrame:
    """
    PROPÓSITO
    ---------
    Detecta y corrige dinámicamente los casos donde un mismo UniqueCarrier
    (código IATA de aerolínea) aparece asociado a más de un AirlineID, lo que
    ocurre cuando el código fue temporalmente heredado tras una fusión o
    adquisición (ej.: 'CO' operando bajo el AirlineID de United post-fusión).

    La detección es 100 % dinámica: no requiere lista hardcodeada de carriers;
    emerge directamente de los datos agrupados.

    PASOS INTERNOS
    --------------
    1. Detectar   – llama a _detectar_carriers_con_multiples_ids() para obtener
                    la lista de carriers anómalos y su AirlineID dominante.
    2. Preparar   – inicializa las columnas auxiliares AirlineID_original e
                    is_merged_carrier antes de modificar nada.
    3. Corregir   – por cada carrier anómalo, ubica las filas con ID no dominante
                    y sobreescribe AirlineID con el valor dominante.
    4. Flaggear   – activa is_merged_carrier = True en esas mismas filas.
    5. Verificar  – comprueba que ningún carrier tenga múltiples IDs post-corrección.

    RESULTADO
    ---------
    El DataFrame conserva TODAS las filas; solo se modifican AirlineID e
    is_merged_carrier en los registros anómalos. El valor original queda
    preservado en AirlineID_original para auditoría y trazabilidad.
    """
    banner("REGLA 9 – Códigos IATA reutilizados (detección dinámica)")
    antes = df.shape[0]

    print(f"\n  Analizando {antes:,} registros...")
    print(f"  Buscando UniqueCarrier asociados a más de un AirlineID...")

    # ── Paso 1: detección dinámica ────────────────────────────────
    # Sin listas hardcodeadas: la función auxiliar agrupa los datos y
    # determina el ID dominante puramente a partir de las frecuencias.
    anomalos = _detectar_carriers_con_multiples_ids(df)

    # ── Paso 2: inicializar columnas auxiliares ───────────────────
    # Hacemos esto ANTES de cualquier modificación para que
    # AirlineID_original siempre refleje el valor crudo de staging.
    df["is_merged_carrier"]  = False          # flag; True solo en anómalos
    df["AirlineID_original"] = df["AirlineID"]  # copia de seguridad para auditoría

    # ── Caso sin anomalías: salida temprana ───────────────────────
    if anomalos.empty:
        print("\n  ✓ No se detectaron carriers con múltiples AirlineID.")
        print("    Todos los UniqueCarrier tienen un único AirlineID → datos consistentes.")
        log_resumen_regla(
            "Regla 9",
            antes,
            df.shape[0],
            "Sin anomalías detectadas. No se modificó ningún registro.",
        )
        return df

    # ── Paso 2b: reporte del análisis ────────────────────────────
    total_carriers_anomalos   = len(anomalos)
    total_registros_universo  = df[df["UniqueCarrier"].isin(anomalos["UniqueCarrier"])].shape[0]

    print(f"\n  Carriers con múltiples AirlineID detectados: {total_carriers_anomalos}")
    print(f"  Registros pertenecientes a esos carriers   : {total_registros_universo:,}  "
          f"({total_registros_universo / antes * 100:.2f} % del dataset)")

    separador()
    print(f"    {'Carrier':<12} {'IDs distintos':>14} {'ID dominante':>14} "
          f"{'Registros (dom.)':>17} {'Pasajeros (dom.)':>17}")
    separador()
    for _, row in anomalos.iterrows():
        print(
            f"    {row['UniqueCarrier']:<12} "
            f"{int(row['ids_distintos']):>14} "
            f"{int(row['id_dominante']):>14} "
            f"{int(row['conteo_dominante']):>17,} "
            f"{int(row['pasajeros_dominante']):>17,}"
        )
    separador()

    # ── Paso 3 y 4: corregir AirlineID y activar flag ────────────
    # Para cada carrier anómalo:
    #   - buscamos registros cuyo AirlineID NO sea el dominante
    #   - los corregimos (AirlineID → dominante)
    #   - los marcamos (is_merged_carrier → True)
    print(f"\n  Aplicando corrección por carrier:")
    total_corregidos = 0

    for _, row in anomalos.iterrows():
        carrier      = row["UniqueCarrier"]
        id_dominante = int(row["id_dominante"])

        # Universo del carrier: cuántos registros tiene en total
        n_carrier_total = (df["UniqueCarrier"] == carrier).sum()

        # Máscara: mismo carrier pero con AirlineID distinto al dominante
        # Estos son los registros "huérfanos" que quedaron con el ID incorrecto
        mascara_no_dominante = (
            (df["UniqueCarrier"] == carrier) &
            (df["AirlineID"]     != id_dominante)
        )
        n_no_dominantes = mascara_no_dominante.sum()
        n_dominantes    = n_carrier_total - n_no_dominantes

        # Aplicar corrección solo si hay registros para modificar
        if n_no_dominantes > 0:
            df.loc[mascara_no_dominante, "AirlineID"]         = id_dominante
            df.loc[mascara_no_dominante, "is_merged_carrier"] = True
            total_corregidos += n_no_dominantes

        print(f"\n    Carrier '{carrier}'  (ID dominante: {id_dominante})")
        print(f"      Registros totales del carrier            : {n_carrier_total:>8,}")
        print(f"      Con AirlineID dominante  → sin cambios   : {n_dominantes:>8,}")
        print(f"      Con AirlineID no dominante → corregidos  : {n_no_dominantes:>8,}")

    # ── Paso 5: verificación cruzada post-corrección ──────────────
    # Confirmamos que ninguno de los carriers antes anómalos sigue
    # teniendo más de un AirlineID en el DataFrame corregido.
    ids_post              = (
        df[df["UniqueCarrier"].isin(anomalos["UniqueCarrier"])]
        .groupby("UniqueCarrier")["AirlineID"]
        .nunique()
    )
    carriers_aun_anomalos = ids_post[ids_post > 1]

    print(f"\n  Verificación post-corrección:")
    if carriers_aun_anomalos.empty:
        print(f"  ✓ OK – Todos los carriers corregidos tienen ahora un único AirlineID.")
        print(f"  ✓ Total registros con is_merged_carrier=True : {total_corregidos:,}")
        print(f"  ✓ AirlineID_original preservado para auditoría en todos los casos.")
    else:
        print(f"  ⚠  Carriers que aún tienen múltiples IDs: "
              f"{list(carriers_aun_anomalos.index)}")

    log_resumen_regla(
        "Regla 9",
        antes,
        df.shape[0],
        f"{total_corregidos:,} registros corregidos (AirlineID → dominante) y "
        f"marcados con is_merged_carrier=True. "
        f"Valor original preservado en 'AirlineID_original'.",
    )
    return df


# ================================================================
# REGLA 10 – Filas duplicadas exactas
# ================================================================

def regla_10_duplicados(df: pd.DataFrame) -> pd.DataFrame:
    """
    PROPÓSITO
    ---------
    Elimina filas que sean duplicados exactos considerando TODAS las columnas
    del DataFrame en su estado actual (incluyendo las columnas nuevas añadidas
    por las reglas anteriores: AirlineID_original e is_merged_carrier).

    CAUSA CONOCIDA
    --------------
    Las recargas mensuales de los CSVs fuente insertan el mismo segmento
    más de una vez en la tabla de staging.  Estos registros son idénticos
    celda por celda y no aportan información adicional al DW.

    CRITERIO
    --------
    keep='first' → se conserva la primera ocurrencia; las demás se eliminan.
    """
    banner("REGLA 10 – Filas duplicadas exactas")
    antes = df.shape[0]

    print(f"\n  Evaluando {antes:,} registros...")
    print(f"  Columnas comparadas: {df.shape[1]} (todas las columnas del DataFrame)")
    print(f"  Criterio           : keep='first' — se conserva la primera ocurrencia")

    # ── Conteo de duplicados antes de eliminar ───────────────────
    # duplicated(keep='first') devuelve True en todas las ocurrencias
    # EXCEPTO la primera, que es exactamente lo que luego borraremos.
    mascara_duplicados = df.duplicated(keep="first")
    n_duplicados       = mascara_duplicados.sum()
    n_unicos           = antes - n_duplicados

    print(f"\n  Filas únicas (primera ocurrencia)    : {n_unicos:>10,}  "
          f"({n_unicos / antes * 100:.2f} %)")
    print(f"  Filas duplicadas (serán eliminadas)  : {n_duplicados:>10,}  "
          f"({n_duplicados / antes * 100:.2f} %)")

    # ── Eliminar duplicados ──────────────────────────────────────
    df = df.drop_duplicates(keep="first")
    n_post = df.shape[0]

    # ── Verificación post-eliminación ───────────────────────────
    # Nos aseguramos de que no quedó ningún duplicado residual.
    restantes_dup = df.duplicated(keep="first").sum()
    print(f"\n  Verificación post-eliminación:")
    if restantes_dup == 0:
        print(f"  ✓ OK – 0 duplicados residuales. Dataset completamente limpio.")
        print(f"  ✓ Filas finales tras deduplicación: {n_post:,}")
    else:
        print(f"  ⚠  Aún quedan {restantes_dup:,} duplicados — revisar lógica.")

    log_resumen_regla(
        "Regla 10",
        antes,
        n_post,
        f"Se eliminaron {n_duplicados:,} filas duplicadas; "
        f"quedaron {n_post:,} filas únicas.",
    )
    return df


# ================================================================
# PIPELINE PRINCIPAL
# ================================================================

def transformar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Orquesta la ejecución de las tres reglas de transformación en orden:
        Regla 8  → limpieza de tiempos imposibles
        Regla 9  → corrección de IDs reutilizados post-fusión
        Regla 10 → eliminación de filas duplicadas exactas

    Recibe el DataFrame crudo leído de staging y devuelve el DataFrame
    limpio y enriquecido, listo para la carga al Data Warehouse.
    """
    banner("INICIO DEL PIPELINE DE TRANSFORMACIÓN")
    print(f"\n  Tabla de origen    : dbo.{STG_TABLE}")
    print(f"  Filas al ingresar  : {df.shape[0]:,}")
    print(f"  Columnas iniciales : {df.shape[1]}")
    print(f"\n  Se aplicarán 3 reglas en secuencia.")

    df = regla_8_tiempos_imposibles(df)
    df = regla_9_iata_reutilizado(df)
    df = regla_10_duplicados(df)

    return df


# ================================================================
# PUNTO DE ENTRADA
# ================================================================

if __name__ == "__main__":
    banner("ETL – TRANSFORMACIÓN DE stg_t100")

    # ── Validar variable de entorno ──────────────────────────────
    if not CONNECTION_STRING:
        print("\nX ERROR: No se encontró DB_CONNECTION_STRING en el archivo .env")
        print("   Asegurate de que el .env esté en la raíz del proyecto.")
        exit(1)

    # ── Crear engine y probar conexión ───────────────────────────
    print("\nConectando a SQL Server...")
    engine = create_engine(CONNECTION_STRING, fast_executemany=True)

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("  ✓ Conexión exitosa")
    except Exception as e:
        print(f"\nX ERROR de conexión: {e}")
        print("   Verificá DB_CONNECTION_STRING en el .env, el driver ODBC y las credenciales.")
        exit(1)

    # ── Leer tabla de staging completa ───────────────────────────
    print(f"\nLeyendo tabla dbo.{STG_TABLE} desde SQL Server...")
    df_raw = pd.read_sql(f"SELECT * FROM dbo.{STG_TABLE}", con=engine)
    print(f"  ✓ Lectura completada: {len(df_raw):,} filas × {df_raw.shape[1]} columnas")

    # ── Ejecutar pipeline de transformación ──────────────────────
    df_clean = transformar(df_raw)

    # ── Resumen ejecutivo final ───────────────────────────────────
    banner("RESUMEN EJECUTIVO – RESULTADO FINAL")

    filas_eliminadas = len(df_raw) - len(df_clean)
    nan_airtime      = df_clean["AirTime"].isna().sum()
    nan_ramptime     = df_clean["RampTime"].isna().sum()
    merged_flag      = int(df_clean["is_merged_carrier"].sum())
    ids_corregidos   = int((df_clean["AirlineID"] != df_clean["AirlineID_original"]).sum())

    print(f"""
  ┌──────────────────────────────────────────────────────────────┐
  │  DIMENSIÓN DEL DATASET                                       │
  │    Filas originales (STG)          : {len(df_raw):>10,}          │
  │    Filas tras transformación (DW)  : {len(df_clean):>10,}          │
  │    Filas netas eliminadas          : {filas_eliminadas:>10,}          │
  ├──────────────────────────────────────────────────────────────┤
  │  REGLA 8 – Tiempos imposibles                                │
  │    AirTime  convertidos a NaN      : {nan_airtime:>10,}          │
  │    RampTime convertidos a NaN      : {nan_ramptime:>10,}          │
  │    Filas conservadas (sin eliminar): {len(df_clean):>10,}          │
  ├──────────────────────────────────────────────────────────────┤
  │  REGLA 9 – Códigos IATA reutilizados                         │
  │    Registros is_merged_carrier=True : {merged_flag:>9,}          │
  │    AirlineID corregidos al dominante: {ids_corregidos:>9,}          │
  │    Filas conservadas (sin eliminar) : {len(df_clean):>9,}          │
  ├──────────────────────────────────────────────────────────────┤
  │  REGLA 10 – Duplicados                                       │
  │    Filas duplicadas eliminadas     : {filas_eliminadas:>10,}          │
  │    Filas únicas resultantes        : {len(df_clean):>10,}          │
  └──────────────────────────────────────────────────────────────┘

  DataFrame listo para carga al DW → variable: df_clean
    """)

    # ── Carga al DW (descomentar cuando el engine de destino esté listo) ──
    #
    # print("Cargando df_clean en el Data Warehouse...")
    # df_clean.to_sql(
    #     name="fact_t100",
    #     con=engine_dw,       # engine apuntando a la base del DW
    #     if_exists="append",
    #     index=False,
    #     chunksize=1000,
    # )
    # print(f"  ✓ {len(df_clean):,} filas cargadas en fact_t100")
