import os
import re
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =============================================================
# 1. CONFIGURACIÓN Y CONEXIONES
# =============================================================
load_dotenv()
CONN_STR_STAGING = os.getenv("DB_CONNECTION_STRING") 
CONN_STR_DW = CONN_STR_STAGING.replace("dw_staging_raw", "dw_trafico_aereo")

engine_stg = create_engine(CONN_STR_STAGING, fast_executemany=True)
engine_dw = create_engine(CONN_STR_DW, fast_executemany=True)

# =============================================================
# 2. FUNCIONES DE LIMPIEZA (Reglas de Calidad 11, 12, 13)
# =============================================================

def aplicar_regla_13_carga(df_datos):
    """
    Descartar registros donde la carga supera la capacidad o es negativa.
    """
    print("  > Aplicando Regla 13 (Validación física de carga)...")
    df_clean = df_datos.copy()
    filas_antes = len(df_clean)
    
    # Filtro excluyente: Carga menor a Capacidad
    df_clean = df_clean[(df_clean['Freight'] < df_clean['Payload']) & (df_clean['Freight'] >= 0)] # & (df_clean['Freight'] >= 0)
    
    descartados = filas_antes - len(df_clean)
    print(f"    - {descartados} registros eliminados por inconsistencia física.")
    return df_clean

def aplicar_regla_12_citymarket(df_datos, engine_stg):
    """
    Corrige el CityMarketID utilizando la tabla l_city_market_id.
    Si el ID está corrupto (es igual al AirportID), cruza con la tabla de lookup 
    para recuperar la descripción de la ciudad y reasignar el mercado correcto.
    """
    print("  > Aplicando Regla 12 (Corrección Avanzada de CityMarketID)...")
    df_clean = df_datos.copy()
    
    # 1. Cargar la tabla maestra de City Markets
    query_cm = "SELECT Code, Description as CityName FROM stg_city_market_ids"
    df_lookup_cm = pd.read_sql(query_cm, engine_stg)
    
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

def aplicar_regla_11_airportseq(df_datos, engine_stg):
    """
    Corrige el AirportSeqID utilizando la tabla l_airport_seq_id.
    Compara de forma segura (numérica) para evitar fallos silenciosos de Pandas 
    y asigna la versión histórica correcta basada en el año del vuelo.
    """
    print("  > Aplicando Regla 11 (Corrección Histórica de AirportSeqID)...")
    df_clean = df_datos.copy()
    
    # 1. Traer y preparar la tabla de lookup
    query_seq = "SELECT Code, Description FROM stg_airport_seq_ids"
    df_lookup = pd.read_sql(query_seq, engine_stg)
    
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
    
    '''
    # =========================================================
    # BLOQUE DE AUDITORÍA Y VISUALIZACIÓN DE CAMBIOS
    # =========================================================
    mask_total = mask_orig_err | mask_dest_err
    
    if mask_total.sum() > 0:
        # Filtramos solo las filas afectadas y seleccionamos las columnas clave para ver el "Antes y Después"
        # Recordá que el AirportID es el dato que venía mal (el "Antes") y el SeqID ahora tiene el dato corregido (el "Después")
        df_audit = df_clean.loc[mask_total, [
            'Year', 
            'OriginAirportID', 'OriginAirportSeqID', 
            'DestAirportID', 'DestAirportSeqID'
        ]]
        
        print("\n--- MUESTRA DE REGISTROS CORREGIDOS (REGLA 11) ---")
        print(df_audit.head(15).to_string()) # to_string() fuerza a Pandas a imprimir sin ocultar columnas
        
        # Exportamos la totalidad de los registros modificados a un CSV para que los revises
        archivo_auditoria = 'auditoria_regla11_seqid.csv'
        df_audit.to_csv(archivo_auditoria, index=False)
        print(f"\n[INFO] Se exportaron los {mask_total.sum()} registros corregidos al archivo: {archivo_auditoria}")
    # =========================================================
    '''

    return df_clean

## Dejo esto porque me sirve para ir haciendo el control de que las reglas funcionen correctamente
## Cuando juntemos todo hay que ver como unificamos

def limpiar_calidad_11_12_13(df_datos):
    """
    Orquestador: Ejecuta las reglas 11, 12 y 13 en cadena.
    """
    print("\nIniciando módulo de calidad (Reglas 11, 12 y 13)...")
    df_procesado = df_datos.copy()
    
    df_procesado = aplicar_regla_13_carga(df_procesado)
    df_procesado = aplicar_regla_12_citymarket(df_procesado, engine_stg)
    df_procesado = aplicar_regla_11_airportseq(df_procesado, engine_stg)
    
    print("  ✓ Módulo de calidad completado.\n")
    return df_procesado

# =============================================================
# 3. EXTRACCIÓN Y EJECUCIÓN
# =============================================================
if __name__ == "__main__":
    print("=============================================================")
    print("1. Extrayendo datos desde dw_staging_raw (engine_stg)...")
    print("=============================================================")
    
    # Usamos tu conexión para traer los datos crudos a Pandas
    query_stg = "SELECT * FROM stg_t100"
    df_vuelos_crudos = pd.read_sql(query_stg, engine_stg)
    print(f"✓ Datos extraídos: {len(df_vuelos_crudos)} registros en total.")
    
    # Pasamos el DataFrame extraído a nuestra función orquestadora
    df_vuelos_limpios = limpiar_calidad_11_12_13(df_vuelos_crudos)
    
    print("=============================================================")
    print("Resumen:")
    print(f"Registros iniciales: {len(df_vuelos_crudos)}")
    print(f"Registros listos para continuar el ETL: {len(df_vuelos_limpios)}")
    print("=============================================================")