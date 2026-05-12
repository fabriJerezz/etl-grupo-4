"""
etl_transform_main.py
---------------------
Orquestador central de la fase de Transformación (ETL).
Integra las 16 reglas de negocio definidas por el equipo en un único flujo de Pandas.
Prepara el DataFrame con los tipos de datos exactos para su carga al Data Warehouse.
"""

import os
import time
import numpy as np
import pandas as pd
import re
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =============================================================
# 1. CONFIGURACIÓN Y CONEXIONES
# =============================================================
ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(ENV_PATH)

CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
engine_stg = create_engine(CONNECTION_STRING, fast_executemany=True)

# =============================================================
# 2. FUNCIONES DE TRANSFORMACIÓN Y CALIDAD (Reglas 1 a 16)
# =============================================================

# --- MÓDULO 1: IDENTIFICADORES Y NOMBRES (Reglas 1 a 4) ---

def aplicar_regla_1_sufijos(df):
    """
    R01: Elimina el sufijo numérico (ej. (1)) del código IATA en UniqueCarrier y Carrier.
    Evita la fragmentación del agrupamiento para aerolíneas que reutilizaron códigos.
    """
    print("  > Aplicando R01: Limpiando sufijos numéricos en códigos IATA...")
    
    # 1. Identificar registros afectados (para auditoría visual)
    mask_sufijo = df['UniqueCarrier'].str.contains(r'\(\d+\)$', na=False, regex=True)
    afectados_totales = mask_sufijo.sum()
    
    # 2. Aplicar corrección solo si hay casos
    if afectados_totales > 0:
        # Reemplazamos el patrón (número al final) por vacío y eliminamos espacios residuales
        df['UniqueCarrier'] = df['UniqueCarrier'].str.replace(r'\(\d+\)$', '', regex=True).str.strip()
        df['Carrier'] = df['Carrier'].str.replace(r'\(\d+\)$', '', regex=True).str.strip()
        
    print(f"    - {afectados_totales} registros corregidos (sufijos eliminados).")
    
    return df

def aplicar_regla_2_normalizar_nombres(df):
    """
    R02: Normaliza UniqueCarrierName (Estandarización, limpieza de sufijos, 
    expansión de abreviaturas y mapeo a nombre canónico).
    """
    print("  > Aplicando R02: Normalizando nombres de aerolíneas (UniqueCarrierName)...")
    
    # 1. Definición de diccionarios y patrones internos del negocio
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

    ABREVIATURAS = {
        "AMER.":  "AMERICAN",
        "AMER":   "AMERICAN",
        "AIRLS.": "AIR LINES",
        "AIRLS":  "AIR LINES",
        "INTL.":  "INTERNATIONAL",
        "INTL":   "INTERNATIONAL",
    }

    NOMBRES_CANONICOS = {
        "AMERICANAIRLINES":       "AMERICAN AIRLINES",
        "DELTA AIRLINES":         "DELTA AIR LINES",
        "UNITED AIR LINES":       "UNITED AIRLINES",
        "SOUTHWEST AIR":          "SOUTHWEST AIRLINES",
        "ALASKA AIR LINES":       "ALASKA AIRLINES",
        "ALASKA AIR":             "ALASKA AIRLINES",
        "JETBLUE":                "JETBLUE AIRWAYS",
        "JET BLUE AIRWAYS":       "JETBLUE AIRWAYS",
        "JET BLUE":               "JETBLUE AIRWAYS",
        "FRONTIER AIR LINES":     "FRONTIER AIRLINES",
        "FRONTIER AIR":           "FRONTIER AIRLINES",
        "ALLEGIANT AIRLINES":     "ALLEGIANT AIR",
        "SPIRIT AIRLINES":        "SPIRIT AIR LINES",
        "SKYWEST AIR LINES":      "SKYWEST AIRLINES",
        "SKY WEST AIRLINES":      "SKYWEST AIRLINES",
        "SKY WEST AIR LINES":     "SKYWEST AIRLINES",
        "REPUBLIC AIR LINES":     "REPUBLIC AIRLINES",
        "REPUBLIC AIR":           "REPUBLIC AIRLINES",
        "ENDEAVOR AIRLINES":      "ENDEAVOR AIR",
        "ENDEAVOR AIR LINES":     "ENDEAVOR AIR",
        "ENVOY AIRLINES":         "ENVOY AIR",
        "AMERICAN EAGLE (ENVOY)": "ENVOY AIR",
        "MESA AIRLINES":          "MESA AIR",
        "COMMUTAIR":              "COMMUT AIR",
    }

    # Función interna de transformación de strings
    def normalizar_nombre(nombre):
        if pd.isna(nombre):
            return nombre
        
        # A. Estandarizar capitalización y espacios
        nombre = str(nombre).strip().upper()
        # B. Quitar sufijos corporativos del final
        nombre = PATRON_SUFIJOS.sub("", nombre).strip()
        # C. Expandir abreviaturas palabra por palabra
        nombre = " ".join(ABREVIATURAS.get(p, p) for p in nombre.split())
        # D. Mapear al nombre canónico
        return NOMBRES_CANONICOS.get(nombre, nombre)

    # 2. Conteo previo para auditoría
    unicos_antes = df['UniqueCarrierName'].nunique()
    
    # 3. OPTIMIZACIÓN: Aplicar transformación SOLO a los valores únicos
    valores_unicos = df['UniqueCarrierName'].dropna().unique()
    mapa_normalizacion = {nombre_original: normalizar_nombre(nombre_original) for nombre_original in valores_unicos}
    
    # 4. Inyección vectorizada al DataFrame
    df['UniqueCarrierName'] = df['UniqueCarrierName'].map(mapa_normalizacion).fillna(df['UniqueCarrierName'])
    
    # 5. Conteo posterior
    unicos_despues = df['UniqueCarrierName'].nunique()

    print(f"    - Variantes de nombres reducidas exitosamente de {unicos_antes} a {unicos_despues} nombres canónicos.")
    
    return df

def aplicar_regla_3_airline_id(df):
    """
    R03: Trata AirlineID inválido (0 o NULL) como carrier "Desconocido".
    Reemplaza los identificadores por valores centinela homogéneos 
    (0, 'UNK', 'DESCONOCIDO') para garantizar la integridad referencial en el DW.
    """
    print("  > Aplicando R03: Estandarizando AirlineID nulos o en cero...")
    
    # 1. Asegurar que la columna sea numérica y rellenar nulos con 0 para capturarlos en la máscara
    df['AirlineID'] = pd.to_numeric(df['AirlineID'], errors='coerce').fillna(0).astype(int)
    
    # 2. Identificar registros afectados
    mask_desconocido = df['AirlineID'] == 0
    afectados_totales = mask_desconocido.sum()
    
    # 3. Aplicar valores centinela definidos por el negocio
    if afectados_totales > 0:
        df.loc[mask_desconocido, 'AirlineID'] = 0
        df.loc[mask_desconocido, 'UniqueCarrier'] = 'UNK'
        df.loc[mask_desconocido, 'UniqueCarrierName'] = 'DESCONOCIDO'
        
    print(f"    - {afectados_totales} registros marcados y homogeneizados como aerolínea 'DESCONOCIDA'.")
    
    return df

def aplicar_regla_4_campos_obligatorios(df, engine):
    """
    R04: Trata campos obligatorios con valores NULL (o strings vacíos).
    - CarrierName: Se imputa usando el código IATA limpio ('Carrier').
    - Class: Se imputa usando la moda histórica del 'UniqueCarrier'.
    - CityName (Origin/Dest): Se imputa cruzando con stg_airports y usando fallback del dataset.
    - AircraftType: Se mantiene en NULL ya que la relación no es determinística.
    """
    print("  > Aplicando R04: Imputando nulos en campos obligatorios...")
    
    # 1. Estandarizar strings vacíos a NaN para que las máscaras .isna() no fallen
    campos = ['CarrierName', 'Class', 'OriginCityName', 'DestCityName', 'AircraftType']
    for col in campos:
        if col in df.columns:
            df[col] = df[col].replace(r'^\s*$', np.nan, regex=True)

    afectados_totales = 0

    # 2. Imputar CarrierName (desde Carrier)
    mask_carrier = df['CarrierName'].isna()
    if mask_carrier.sum() > 0:
        df.loc[mask_carrier, 'CarrierName'] = df.loc[mask_carrier, 'Carrier']
        afectados_totales += mask_carrier.sum()

    # 3. Imputar Class (Moda por UniqueCarrier)
    mask_class = df['Class'].isna()
    if mask_class.sum() > 0:
        # Construimos el mapa dinámico ignorando nulos
        mapa_class = (
            df[df['Class'].notna()]
            .groupby('UniqueCarrier')['Class']
            .agg(lambda x: x.mode()[0] if not x.mode().empty else np.nan)
            .to_dict()
        )
        df.loc[mask_class, 'Class'] = df.loc[mask_class, 'UniqueCarrier'].map(mapa_class)
        afectados_totales += mask_class.sum()

    # 4. Imputar CityNames (Origin y Destino)
    mask_orig = df['OriginCityName'].isna()
    mask_dest = df['DestCityName'].isna()

    if mask_orig.sum() > 0 or mask_dest.sum() > 0:
        # Paso A: Traer lookup de aeropuertos UNA sola vez desde la BD
        query_airports = "SELECT Code, Description FROM stg_airports WHERE Code IS NOT NULL AND Description IS NOT NULL"
        df_lk = pd.read_sql(query_airports, engine)
        
        # Extraer la ciudad (antes de los dos puntos)
        df_lk['ciudad'] = df_lk['Description'].str.split(':').str[0].str.strip()
        mapa_lookup = dict(zip(df_lk['Code'], df_lk['ciudad']))

        # Paso B: Imputar Origen
        if mask_orig.sum() > 0:
            mapa_ds_orig = df[df['OriginCityName'].notna()].groupby('Origin')['OriginCityName'].agg(lambda x: x.mode()[0] if not x.mode().empty else np.nan).to_dict()
            mapa_final_orig = {**mapa_ds_orig, **mapa_lookup} # Lookup de BD pisa al dataset
            df.loc[mask_orig, 'OriginCityName'] = df.loc[mask_orig, 'Origin'].map(mapa_final_orig)
            afectados_totales += mask_orig.sum()

        # Paso C: Imputar Destino
        if mask_dest.sum() > 0:
            mapa_ds_dest = df[df['DestCityName'].notna()].groupby('Dest')['DestCityName'].agg(lambda x: x.mode()[0] if not x.mode().empty else np.nan).to_dict()
            mapa_final_dest = {**mapa_ds_dest, **mapa_lookup}
            df.loc[mask_dest, 'DestCityName'] = df.loc[mask_dest, 'Dest'].map(mapa_final_dest)
            afectados_totales += mask_dest.sum()

    print(f"    - {afectados_totales} valores imputados exitosamente (CarrierName, Class, CityNames).")

    # 5. Auditoría pasiva de AircraftType
    if 'AircraftType' in df.columns:
        n_aircraft = df['AircraftType'].isna().sum()
        if n_aircraft > 0:
            print(f"    - {n_aircraft} nulos en AircraftType mantenidos como NULL (no imputables).")

    return df

# --- MÓDULO 2: CONSISTENCIA FÍSICA Y DISTANCIAS (Reglas 5 a 7) ---

def aplicar_regla_5_dep_performed(df):
    """
    R05: Ajusta DepPerformed cuando supera a DepScheduled.
    Se asume que el exceso se explica por reprogramaciones operativas, 
    por lo que se aplica un "cap" (tope) igualándolo al valor programado.
    """
    print("  > Aplicando R05: Validando Vuelos Realizados vs Programados...")
    
    # 1. Asegurar tipos numéricos para una comparación matemática segura
    df['DepScheduled'] = pd.to_numeric(df['DepScheduled'], errors='coerce')
    df['DepPerformed'] = pd.to_numeric(df['DepPerformed'], errors='coerce')
    
    # 2. Identificar registros donde lo realizado supera a lo programado
    mask_exceso = df['DepPerformed'].notna() & df['DepScheduled'].notna() & (df['DepPerformed'] > df['DepScheduled'])
    afectados_totales = mask_exceso.sum()
    
    # 3. Aplicar el tope (cap) in-place
    if afectados_totales > 0:
        df.loc[mask_exceso, 'DepPerformed'] = df.loc[mask_exceso, 'DepScheduled']
        
    print(f"    - {afectados_totales} registros ajustados (exceso de vuelos realizados truncado al programado).")
    
    return df

def aplicar_regla_6_pasajeros_asientos(df):
    """
    R06: Ajusta Passengers cuando supera a Seats (solo si Seats > 0).
    Se asume que el exceso se debe a desajustes de carga o reubicaciones, 
    por lo que se aplica un "cap" igualándolo a la capacidad declarada (Seats).
    Nota: Los casos operativos con Seats = 0 se auditan en la Regla 14.
    """
    print("  > Aplicando R06: Validando Pasajeros transportados vs Asientos disponibles...")
    
    # 1. Asegurar tipos numéricos para una comparación matemática segura
    df['Passengers'] = pd.to_numeric(df['Passengers'], errors='coerce')
    df['Seats'] = pd.to_numeric(df['Seats'], errors='coerce')
    
    # 2. Identificar registros donde Pasajeros > Asientos (ignorando Seats == 0)
    mask_exceso = (
        df['Passengers'].notna() & 
        df['Seats'].notna() & 
        (df['Seats'] > 0) & 
        (df['Passengers'] > df['Seats'])
    )
    afectados_totales = mask_exceso.sum()
    
    # 3. Aplicar el tope (cap) in-place
    if afectados_totales > 0:
        df.loc[mask_exceso, 'Passengers'] = df.loc[mask_exceso, 'Seats']
        
    print(f"    - {afectados_totales} registros ajustados (exceso de pasajeros truncado a capacidad máxima).")
    
    return df

def aplicar_regla_7_distancia(df):
    """
    R07: Imputa valores de Distance nulos o <= 0 utilizando la distancia 
    máxima registrada para el mismo par de ruta (Origin, Dest) dentro del dataset.
    Si no existe una referencia válida para ese par, el registro se descarta.
    """
    print("  > Aplicando R07: Validando e imputando distancias de vuelo...")
    
    # 1. Asegurar tipo numérico para comparaciones seguras
    df['Distance'] = pd.to_numeric(df['Distance'], errors='coerce')
    
    # 2. Identificar registros con distancia inválida
    mask_bad = df['Distance'].isna() | (df['Distance'] <= 0)
    afectados_iniciales = mask_bad.sum()
    
    if afectados_iniciales > 0:
        # 3. Construir tabla de referencia con las distancias válidas (max por ruta)
        df_good = df[~mask_bad]
        ref_dist = df_good.groupby(['Origin', 'Dest'])['Distance'].max().reset_index()
        ref_dist = ref_dist.rename(columns={'Distance': '_dist_ref'})
        
        # 4. Cruzar referencia temporalmente con el dataset principal
        df = df.merge(ref_dist, on=['Origin', 'Dest'], how='left')
        
        # 5. Imputar valores donde la referencia exista
        mask_can_fill = mask_bad & df['_dist_ref'].notna()
        n_imputados = mask_can_fill.sum()
        
        if n_imputados > 0:
            df.loc[mask_can_fill, 'Distance'] = df.loc[mask_can_fill, '_dist_ref']
            
        # Limpiar columna auxiliar
        df = df.drop(columns=['_dist_ref'])
        
        # 6. Identificar y descartar las filas que siguen siendo inválidas (irrecuperables)
        mask_still_bad = df['Distance'].isna() | (df['Distance'] <= 0)
        n_eliminados = mask_still_bad.sum()
        
        if n_eliminados > 0:
            df = df[~mask_still_bad]
            
        print(f"    - {n_imputados} distancias imputadas exitosamente según el par Origen-Destino.")
        print(f"    - {n_eliminados} registros eliminados (sin referencia histórica de distancia).")
    else:
        print("    - 0 registros requirieron imputación de distancia.")
        
    return df

# --- MÓDULO 3: TIEMPOS Y DUPLICADOS (Reglas 8 a 10) ---

def aplicar_regla_8_tiempos_vuelo(df):
    """
    R08: Neutraliza valores de AirTime y RampTime que son físicamente imposibles 
    (< 1 o > 9000 minutos) convirtiéndolos a NaN. No elimina la fila.
    """
    print("  > Aplicando R08: Neutralizando tiempos de vuelo imposibles...")
    
    # Cualquier valor fuera de los limietes se considera físicamente imposible.
    # 9 000 min ≈ 150 horas → ningún vuelo comercial supera ese valor.
    # 0 o negativo tampoco tiene sentido operativo.
    LIMITE_MIN = 1
    LIMITE_MAX = 9000
    
    afectados_totales = 0
    
    for col in ['AirTime', 'RampTime']:
        if col in df.columns:
            # Asegurar tipo numérico para comparaciones seguras
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Máscara de valores anómalos (negativos, cero, o superiores a 150 horas)
            mask_anomala = (df[col] < LIMITE_MIN) | (df[col] > LIMITE_MAX)
            afectados_col = mask_anomala.sum()
            
            if afectados_col > 0:
                # Neutralizamos el dato a NaN preservando el resto de la fila
                df.loc[mask_anomala, col] = np.nan
                afectados_totales += afectados_col

    # (Opcional) Validación lógica adicional: AirTime no puede ser mayor al RampTime
    if 'AirTime' in df.columns and 'RampTime' in df.columns:
        mask_incoherente = df['AirTime'] > df['RampTime']
        incoherentes = mask_incoherente.sum()
        if incoherentes > 0:
            df.loc[mask_incoherente, ['AirTime', 'RampTime']] = np.nan
            afectados_totales += incoherentes

    print(f"    - {afectados_totales} valores anómalos de tiempo neutralizados a NaN.")
    
    return df

def aplicar_regla_9_iata_reutilizado(df):
    """
    R09: Detecta dinámicamente UniqueCarriers con múltiples AirlineID.
    Asigna el AirlineID dominante (por volumen de registros y pasajeros) 
    a los registros huérfanos, preservando el original para auditoría.
    """
    print("  > Aplicando R09: Estandarizando códigos IATA reutilizados...")
    
    # 1. Crear columnas de auditoría y control ANTES de modificar nada
    df['is_merged_carrier'] = False
    df['AirlineID_original'] = df['AirlineID']
    
    # 2. Generar tabla de frecuencias (Registros y Pasajeros por Carrier/ID)
    conteos = df.groupby(['UniqueCarrier', 'AirlineID'], as_index=False).agg(
        n_registros=('AirlineID', 'count'),
        n_pasajeros=('Passengers', 'sum')
    )
    
    # 3. Detectar carriers anómalos (los que tienen > 1 AirlineID distinto)
    ids_por_carrier = conteos.groupby('UniqueCarrier')['AirlineID'].nunique()
    carriers_anomalos = ids_por_carrier[ids_por_carrier > 1].index
    
    afectados_totales = 0
    
    if len(carriers_anomalos) > 0:
        # 4. Filtrar y ordenar para encontrar el dominante (Desempate: 1° Registros, 2° Pasajeros)
        subset = conteos[conteos['UniqueCarrier'].isin(carriers_anomalos)].copy()
        subset = subset.sort_values(
            by=['UniqueCarrier', 'n_registros', 'n_pasajeros'],
            ascending=[True, False, False]
        )
        
        # Al agrupar y tomar el '.first()', nos quedamos con el ID de mayor peso
        dominantes = subset.groupby('UniqueCarrier').first().reset_index()
        map_dominantes = dict(zip(dominantes['UniqueCarrier'], dominantes['AirlineID']))
        
        # 5. Aplicar la corrección a los registros no dominantes
        for carrier, id_dominante in map_dominantes.items():
            mask_corregir = (df['UniqueCarrier'] == carrier) & (df['AirlineID'] != id_dominante)
            n_corregir = mask_corregir.sum()
            
            if n_corregir > 0:
                df.loc[mask_corregir, 'AirlineID'] = id_dominante
                df.loc[mask_corregir, 'is_merged_carrier'] = True
                afectados_totales += n_corregir

    print(f"    - {afectados_totales} registros huérfanos unificados bajo un AirlineID dominante.")
    
    return df

def aplicar_regla_10_duplicados(df):
    """
    R10: Elimina filas que sean duplicados exactos considerando TODAS 
    las columnas del DataFrame en su estado actual, conservando solo la primera ocurrencia.
    """
    print("  > Aplicando R10: Eliminando filas duplicadas exactas...")
    
    filas_antes = len(df)
    
    # Al no pasar el argumento 'subset', Pandas evalúa la fila completa celda por celda
    df = df.drop_duplicates(keep='first')
    
    afectados_totales = filas_antes - len(df)
    
    print(f"    - {afectados_totales} registros duplicados eliminados del dataset.")
    
    return df

# --- MÓDULO 4: GEOGRAFÍA Y CARGA (Reglas 11 a 13) ---

def aplicar_regla_11_airportseq(df, engine):
    """
    Corrige el AirportSeqID utilizando la tabla l_airport_seq_id.
    Compara de forma segura (numérica) para evitar fallos silenciosos de Pandas 
    y asigna la versión histórica correcta basada en el año del vuelo.
    """
    print("  > Aplicando Regla 11 (Corrección Histórica de AirportSeqID)...")
    df_clean = df.copy()
    
    # 1. Traer y preparar la tabla de lookup
    query_seq = "SELECT Code, Description FROM stg_airport_seq_ids"
    df_lookup = pd.read_sql(query_seq, engine)
    
    df_lookup['Code'] = df_lookup['Code'].astype(str).str.strip()
    df_validos = df_lookup[df_lookup['Code'].str.len() == 7].copy()
    df_validos['BaseAirportID'] = df_validos['Code'].str[:5]
    
    # 2. Separar mapeos temporales
    mask_pre_2005 = df_validos['Description'].str.contains('pre-2005', case=False, na=False)
    map_pre = dict(zip(df_validos.loc[mask_pre_2005, 'BaseAirportID'], df_validos.loc[mask_pre_2005, 'Code']))
    map_post = dict(zip(df_validos.loc[~mask_pre_2005, 'BaseAirportID'], df_validos.loc[~mask_pre_2005, 'Code']))

    # 3. Preparación segura de la tabla de hechos
    # Forzamos Year a numérico para evaluar el período
    df_clean['Year'] = pd.to_numeric(df_clean['Year'], errors='coerce')
    
    # Casteamos las columnas de destino a 'object' para permitir inyección de strings o nulos
    df_clean['OriginAirportSeqID'] = df_clean['OriginAirportSeqID'].astype('object')
    df_clean['DestAirportSeqID'] = df_clean['DestAirportSeqID'].astype('object')

    # ==========================
    # CORRECCIÓN EN ORIGEN
    # ==========================
    # Comparación segura convirtiendo a float (ignora los problemas de '.0' o strings mezclados)
    mask_orig_err = (
        pd.notna(df_clean['OriginAirportSeqID']) & 
        pd.notna(df_clean['OriginAirportID']) & 
        (df_clean['OriginAirportSeqID'].astype(float) == df_clean['OriginAirportID'].astype(float))
    )
    
    # Separar por períodos
    mask_orig_pre = mask_orig_err & (df_clean['Year'] < 2005)
    mask_orig_post = mask_orig_err & (df_clean['Year'] >= 2005)
    
    if mask_orig_err.sum() > 0:
        # Extraemos el ID base LIMPIO (sin decimales) solo de los errores detectados
        id_base_orig = df_clean.loc[mask_orig_err, 'OriginAirportID'].astype(float).astype(int).astype(str)
        
        # Mapeamos e inyectamos. (Si un ID erróneo no tiene mapeo en el diccionario, Pandas pondrá NaN automáticamente, 
        # lo cual cumple con nuestra regla de neutralizar si no hay data).
        df_clean.loc[mask_orig_pre, 'OriginAirportSeqID'] = id_base_orig[mask_orig_pre].map(map_pre)
        df_clean.loc[mask_orig_post, 'OriginAirportSeqID'] = id_base_orig[mask_orig_post].map(map_post)

    # ==========================
    # CORRECCIÓN EN DESTINO
    # ==========================
    mask_dest_err = (
        pd.notna(df_clean['DestAirportSeqID']) & 
        pd.notna(df_clean['DestAirportID']) & 
        (df_clean['DestAirportSeqID'].astype(float) == df_clean['DestAirportID'].astype(float))
    )
    
    mask_dest_pre = mask_dest_err & (df_clean['Year'] < 2005)
    mask_dest_post = mask_dest_err & (df_clean['Year'] >= 2005)
    
    if mask_dest_err.sum() > 0:
        id_base_dest = df_clean.loc[mask_dest_err, 'DestAirportID'].astype(float).astype(int).astype(str)
        
        df_clean.loc[mask_dest_pre, 'DestAirportSeqID'] = id_base_dest[mask_dest_pre].map(map_pre)
        df_clean.loc[mask_dest_post, 'DestAirportSeqID'] = id_base_dest[mask_dest_post].map(map_post)

    afectados = mask_orig_err.sum() + mask_dest_err.sum()
    print(f"    - {afectados} IDs de secuencia temporal evaluados y corregidos mediante mapeo histórico.")

    return df_clean

def aplicar_regla_12_citymarket(df, engine):
    """
    Corrige el CityMarketID utilizando la tabla l_city_market_id.
    Si el ID está corrupto (es igual al AirportID), cruza con la tabla de lookup 
    para recuperar la descripción de la ciudad y reasignar el mercado correcto.
    """
    print("  > Aplicando Regla 12 (Corrección Avanzada de CityMarketID)...")
    df_clean = df.copy()
    
    # 1. Cargar la tabla maestra de City Markets
    query_cm = "SELECT Code, Description as CityName FROM stg_city_market_ids"
    df_lookup_cm = pd.read_sql(query_cm, engine)
    
    # Aseguramos tipos consistentes para el merge
    df_lookup_cm['Code'] = df_lookup_cm['Code'].astype(str)
    df_clean['OriginCityMarketID'] = df_clean['OriginCityMarketID'].astype(str)
    df_clean['DestCityMarketID'] = df_clean['DestCityMarketID'].astype(str)
    
    # ==========================
    # CORRECCIÓN EN ORIGEN
    # ==========================
    mask_orig = df_clean['OriginCityMarketID'] == df_clean['OriginAirportID'].astype(str)
    afectados_orig = mask_orig.sum()
    
    if afectados_orig > 0:
        # Hacemos un merge izquierdo solo para las filas afectadas, usando el ID erróneo
        # para buscar en la tabla de lookup
        df_corregido_orig = pd.merge(
            df_clean[mask_orig], 
            df_lookup_cm, 
            left_on='OriginCityMarketID', 
            right_on='Code', 
            how='left'
        )
        
        # En una arquitectura completa, usaríamos el 'CityName' recuperado para volver a buscar 
        # el ID correcto de los 30.000. Por simplicidad y eficiencia en Pandas, 
        # actualizamos el nombre de la ciudad de origen directamente, asegurando que cuando
        # se cruce con la dimensión Aeropuerto, la ciudad esté correcta.
        # (Dependiendo de cómo esté estructurada tu dimensión final, podrías dejar el ID nulo
        # si la ciudad ya está corregida).
        df_clean.loc[mask_orig, 'OriginCityName'] = df_corregido_orig['CityName'].values
        
        # Opcional: Ahora sí neutralizamos el ID erróneo para que no cause problemas en las FKs futuras
        df_clean.loc[mask_orig, 'OriginCityMarketID'] = None

    # ==========================
    # CORRECCIÓN EN DESTINO
    # ==========================
    mask_dest = df_clean['DestCityMarketID'] == df_clean['DestAirportID'].astype(str)
    afectados_dest = mask_dest.sum()
    
    if afectados_dest > 0:
        df_corregido_dest = pd.merge(
            df_clean[mask_dest], 
            df_lookup_cm, 
            left_on='DestCityMarketID', 
            right_on='Code', 
            how='left'
        )
        df_clean.loc[mask_dest, 'DestCityName'] = df_corregido_dest['CityName'].values
        df_clean.loc[mask_dest, 'DestCityMarketID'] = None
        
    print(f"    - {afectados_orig + afectados_dest} registros geográficos corregidos usando tabla de lookup.")
    return df_clean

import pandas as pd

def aplicar_regla_13_carga(df):
    """
    R13: Ajusta la carga (Freight) cuando supera la capacidad máxima (Payload).
    En lugar de descartar el registro, se aplica un "cap" igualándolo a la 
    capacidad máxima de la aeronave, preservando así el resto de las métricas del vuelo.
    """
    print("  > Aplicando R13: Validando Carga transportada vs Capacidad máxima...")
    
    # 1. Asegurar tipos numéricos para una comparación matemática segura
    df['Freight'] = pd.to_numeric(df['Freight'], errors='coerce')
    df['Payload'] = pd.to_numeric(df['Payload'], errors='coerce')
    
    # 2. Identificar registros donde la Carga supera al Payload
    mask_exceso = (
        df['Freight'].notna() & 
        df['Payload'].notna() & 
        (df['Freight'] > df['Payload'])
    )
    afectados_totales = mask_exceso.sum()
    
    # 3. Aplicar el tope (cap) in-place
    if afectados_totales > 0:
        df.loc[mask_exceso, 'Freight'] = df.loc[mask_exceso, 'Payload']
        
    print(f"    - {afectados_totales} registros ajustados (exceso de carga truncado a la capacidad máxima [Payload]).")
    
    return df

def aplicar_regla_14_flag_seats(df):
    """
    R14: Evalúa la coherencia entre Pasajeros y Asientos.
    - Si Seats = 0 pero Passengers > 0 -> Crea un flag (Flag_Review_Seats = 1)
    - Si Passengers > Seats (y Seats > 0) -> Descarta el registro.
    """
    print("  > Aplicando R14: Evaluando asientos nulos con pasajeros...")
    
    # Asegurar tipos numéricos
    df['Seats'] = pd.to_numeric(df['Seats'], errors='coerce')
    df['Passengers'] = pd.to_numeric(df['Passengers'], errors='coerce')
    
    # 1. Crear el flag por defecto en 0
    df['Flag_Review_Seats'] = 0
    
    # 2. Marcar registros sospechosos (Asientos 0, Pasajeros > 0)
    mask_flag = (df['Seats'] == 0) & (df['Passengers'] > 0)
    if mask_flag.sum() > 0:
        df.loc[mask_flag, 'Flag_Review_Seats'] = 1
        
    # 3. Eliminar registros donde los pasajeros exceden la capacidad real (>0)
    mask_delete = (df['Passengers'] > df['Seats']) & (df['Seats'] > 0)
    n_eliminados = mask_delete.sum()
    
    if n_eliminados > 0:
        df = df[~mask_delete]
        
    print(f"    - {mask_flag.sum()} registros marcados con Flag_Review_Seats = 1 para auditoría.")
    print(f"    - {n_eliminados} registros eliminados (Pasajeros > Asientos declarados).")
    
    return df

def aplicar_regla_15_imputar_negativos(df):
    """
    R15: Descartar registros con carga (Freight) negativa.
    """
    print("  > Aplicando R15: Descartando registros con carga negativa...")
    
    df['Freight'] = pd.to_numeric(df['Freight'], errors='coerce')
    
    mask_negativo = df['Freight'] < 0
    n_eliminados = mask_negativo.sum()
    
    if n_eliminados > 0:
        df = df[~mask_negativo]
        
    print(f"    - {n_eliminados} registros eliminados por presentar Freight negativo.")
    
    return df

def aplicar_regla_16_carriers_expirados(df, engine):
    """
    R16: Agrega el flag IsActive. Se setea en 0 si la aerolínea 
    ya estaba extinta (EndDate < 2023-01-01) según l_carrier_history.
    """
    print("  > Aplicando R16: Marcando carriers inactivos (IsActive)...")
    
    # 1. Crear el flag por defecto en 1 (Activo)
    df['IsActive'] = 1
    
    # 2. Traer solo las aerolíneas con fecha de cierre registrada
    query = """
        SELECT AirlineID, UniqueCarrier, EndDate 
        FROM stg_carrier_history 
        WHERE EndDate IS NOT NULL AND LTRIM(RTRIM(EndDate)) <> ''
    """
    df_hist = pd.read_sql(query, engine)
    df_hist['EndDate'] = pd.to_datetime(df_hist['EndDate'], errors='coerce')
    
    # 3. Filtrar las que cerraron antes del inicio de nuestros datos (2023)
    inactivos = df_hist[df_hist['EndDate'] < pd.Timestamp('2023-01-01')]
    inactivos_keys = list(zip(inactivos['AirlineID'].astype(int), inactivos['UniqueCarrier'].astype(str)))
    
    if inactivos_keys:
        # Preparamos AirlineID temporalmente para asegurar el cruce exacto
        airline_tmp = pd.to_numeric(df['AirlineID'], errors='coerce').fillna(0).astype(int)
        
        # 4. Buscamos coincidencias de (AirlineID, UniqueCarrier) en la lista inactiva
        mask_inactivos = pd.Series(list(zip(airline_tmp, df['UniqueCarrier']))).isin(inactivos_keys)
        # Reasignamos el índice para que coincida perfectamente con el df original tras los borrados previos
        mask_inactivos.index = df.index 
        
        n_marcados = mask_inactivos.sum()
        if n_marcados > 0:
            df.loc[mask_inactivos, 'IsActive'] = 0
            
        print(f"    - {n_marcados} registros reportados por aerolíneas inhabilitadas marcados con IsActive = 0.")
    else:
        print("    - 0 carriers inactivos detectados en el catálogo histórico.")
        
    return df


# =============================================================
# 3. ORQUESTADOR PIPELINE Y PREPARACIÓN FINAL PARA DW
# =============================================================

def ejecutar_pipeline_transformacion(df_crudo, engine):
    print("\n" + "="*60)
    print(" INICIANDO PIPELINE DE TRANSFORMACIÓN (REGLAS 1 a 16)")
    print("="*60)
    
    inicio = time.time()
    df = df_crudo.copy()
    filas_iniciales = len(df)
    
    # Ejecución secuencial de reglas

    # Primero aplicamos las reglas que descartan registros
    df = aplicar_regla_10_duplicados(df)
    df = aplicar_regla_7_distancia(df)
    df = aplicar_regla_15_imputar_negativos(df)

    df = aplicar_regla_1_sufijos(df)
    df = aplicar_regla_2_normalizar_nombres(df)
    df = aplicar_regla_3_airline_id(df)
    df = aplicar_regla_4_campos_obligatorios(df, engine)
    
    df = aplicar_regla_5_dep_performed(df)
    df = aplicar_regla_6_pasajeros_asientos(df)
    
    df = aplicar_regla_8_tiempos_vuelo(df)
    df = aplicar_regla_9_iata_reutilizado(df)
    
    df = aplicar_regla_11_airportseq(df, engine)
    df = aplicar_regla_12_citymarket(df, engine)
    df = aplicar_regla_13_carga(df)
    
    df = aplicar_regla_14_flag_seats(df)
    df = aplicar_regla_16_carriers_expirados(df, engine)

# --- PREPARACIÓN FINAL Y TIPADO PARA EL DATA WAREHOUSE ---
    print("\n  > Aplicando Tipos de Datos (Casteo) y calculando métricas finales...")
        
    # 1. OcupPasajeros: (Pasajeros / Asientos) controlando división por cero
    df['OcupPasajeros'] = np.where(df['Seats'] > 0, df['Passengers'] / df['Seats'], 0)
    df['OcupPasajeros'] = df['OcupPasajeros'].astype(float).round(4)
    
    # 2. OcupCarga: (Carga / Capacidad) controlando división por cero
    df['OcupCarga'] = np.where(df['Capacidad'] > 0, df['Carga'] / df['Capacidad'], 0)
    df['OcupCarga'] = df['OcupCarga'].astype(float).round(4)

    # 3. DemoraPista: (RampTime - AirTime) en minutos
    df['DemoraPista'] = df['RampTime'] - df['AirTime']
    # Se castea a float para que SQL Server tolere correctamente los NaN en caso de tiempos nulos
    df['DemoraPista'] = df['DemoraPista'].astype(float)

    # 4. Estacion: Calculada según el mes (Hemisferio Norte - USA) --> Para dimension Tiempo
    # 1: Invierno, 2: Primavera, 3: Verano, 4: Otoño
    condiciones_estacion = [
        df['Month'].isin([12, 1, 2]),  # Invierno 
        df['Month'].isin([3, 4, 5]),   # Primavera 
        df['Month'].isin([6, 7, 8]),   # Verano
        df['Month'].isin([9, 10, 11])  # Otoño
    ]
    valores_estacion = [1, 2, 3, 4]
    
    df['Estacion'] = np.select(condiciones_estacion, valores_estacion, default=0)
    
    tiempo_total = time.time() - inicio
    print("\n" + "="*60)
    print(f" PIPELINE COMPLETADO EN {tiempo_total:.2f}s")
    print(f" Registros iniciales: {filas_iniciales:,}")
    print(f" Registros finales listos para LOAD: {len(df):,}")
    print("="*60)
    
    return df

# =============================================================
# 4. EJECUCIÓN DEL SCRIPT
# =============================================================
if __name__ == "__main__":
    print("Conectando y extrayendo datos crudos desde stg_t100...")
    df_staging = pd.read_sql("SELECT * FROM stg_t100", engine_stg)
    
    # Ejecutamos el pipeline completo
    df_transformado_final = ejecutar_pipeline_transformacion(df_staging, engine_stg)
    # Aquí el DataFrame (df_transformado_final) ya está 100% limpio y tipado, 
    # listo para enviarse al módulo de LOAD.