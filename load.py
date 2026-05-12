import os
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine, text
from transform import ejecutar_pipeline_transformacion

# =============================================================
# 1. CONFIGURACIÓN DE CONEXIONES
# =============================================================
# Se asume la existencia de las variables en el archivo .env
# DB_STAGING: dw_staging_raw | DB_DW: dw_trafico_aereo
CONN_STAGING = os.getenv("DB_CONNECTION_STRING")
CONN_DW = CONN_STAGING.replace("dw_staging_raw", "dw_trafico_aereo")

engine_stg = create_engine(CONN_STAGING)
engine_dw = create_engine(CONN_DW, fast_executemany=True)

def carga_inicial_dw():

    inicio = time.time()

    # --- PASO 1: Obtención de Datos Transformados ---
    print("Iniciando extracción y transformación...")
    df_stg = pd.read_sql("SELECT * FROM stg_t100", engine_stg)
    df = ejecutar_pipeline_transformacion(df_stg, engine_stg)

# --- PASO 2: Limpieza de la Instancia de Desarrollo (Truncate/Delete) ---
    print("\nLimpiando tablas del Data Warehouse para carga inicial...")
    with engine_dw.begin() as conn:
        # 1. Borramos la tabla de hechos (hija)
        conn.execute(text("DELETE FROM Vuelo;"))
        # 2. Borramos las dimensiones (padres)
        conn.execute(text("DELETE FROM Tiempo;"))
        conn.execute(text("DELETE FROM Transportista;"))
        conn.execute(text("DELETE FROM Aeropuerto;"))
        
        # 3. Reiniciar TODOS los contadores IDENTITY a 0
        conn.execute(text("DBCC CHECKIDENT ('Vuelo', RESEED, 0);"))
        conn.execute(text("DBCC CHECKIDENT ('Tiempo', RESEED, 0);"))
        conn.execute(text("DBCC CHECKIDENT ('Transportista', RESEED, 0);"))
        conn.execute(text("DBCC CHECKIDENT ('Aeropuerto', RESEED, 0);"))

    # --- PASO 3: Carga de Dimensiones ---
    
    # A. Dimensión Tiempo
    print("Cargando Dimensión Tiempo...")
    mapa_estaciones = {1: 'Invierno', 2: 'Primavera', 3: 'Verano', 4: 'Otoño'}
    df_tiempo = df[['Year', 'Month', 'Estacion']].drop_duplicates().copy()
    df_tiempo['Estacion_Label'] = df_tiempo['Estacion'].map(mapa_estaciones)
    df_tiempo_final = df_tiempo[['Month', 'Estacion_Label', 'Year']].rename(
        columns={'Month': 'Mes', 'Estacion_Label': 'Estacion', 'Year': 'Anio'}
    )
    df_tiempo_final.to_sql('Tiempo', engine_dw, if_exists='append', index=False)

    # B. Dimensión Aeropuerto
    print("Cargando Dimensión Aeropuerto...")
    orig = df[['Origin', 'OriginCityName']].rename(columns={'Origin': 'Nombre', 'OriginCityName': 'Ciudad'})
    dest = df[['Dest', 'DestCityName']].rename(columns={'Dest': 'Nombre', 'DestCityName': 'Ciudad'})
    df_aero = pd.concat([orig, dest])
    # FORZAMOS UNICIDAD SÓLO POR CÓDIGO IATA (Nombre)
    df_aero = df_aero.drop_duplicates(subset=['Nombre'], keep='first').copy()
    df_aero['Pais'] = 'USA'
    df_aero['Fecha_Inicio'] = pd.Timestamp.now().date()
    df_aero['Fila_Activa'] = 1
    df_aero.to_sql('Aeropuerto', engine_dw, if_exists='append', index=False)

    # C. Dimensión Transportista
    print("Cargando Dimensión Transportista...")
    df_trans = df[['UniqueCarrierName', 'CarrierGroupNew', 'CarrierRegion']]
    # FORZAMOS UNICIDAD SÓLO POR NOMBRE DE AEROLÍNEA
    df_trans = df_trans.drop_duplicates(subset=['UniqueCarrierName'], keep='first').copy()
    df_trans.columns = ['CarrierNombre', 'CarrierGrupo', 'Region']
    df_trans['Fecha_Inicio'] = pd.Timestamp.now().date()
    df_trans['Fila_Activa'] = 1
    df_trans['CarrierGrupo'] = df_trans['CarrierGrupo'].astype(str)
    df_trans.to_sql('Transportista', engine_dw, if_exists='append', index=False)

    # --- PASO 4: Mapeo de Surrogate Keys para Tabla de Hechos ---
    print("\nRecuperando llaves subrogadas y mapeando tabla de hechos...")
    
    # Traemos SÓLO las llaves y el identificador para evitar que otras columnas generen duplicidad
    dim_tiempo = pd.read_sql("SELECT TiempoKey, Mes, Anio FROM Tiempo", engine_dw)
    dim_aero = pd.read_sql("SELECT AeropuertoKey, Nombre FROM Aeropuerto", engine_dw)
    dim_trans = pd.read_sql("SELECT TransportistaKey, CarrierNombre FROM Transportista", engine_dw)

    # Mapeo Tiempo
    df = df.merge(dim_tiempo, left_on=['Month', 'Year'], right_on=['Mes', 'Anio'], how='left')

    # Mapeo Transportista
    df = df.merge(dim_trans, left_on='UniqueCarrierName', right_on='CarrierNombre', how='left')

    # Mapeo Aeropuerto Origen (solo por código IATA)
    df = df.merge(dim_aero, left_on='Origin', right_on='Nombre', how='left')
    df = df.rename(columns={'AeropuertoKey': 'AeropuertoOrigenKey'}).drop(columns=['Nombre'])
    
    # Mapeo Aeropuerto Destino (solo por código IATA)
    df = df.merge(dim_aero, left_on='Dest', right_on='Nombre', how='left')
    df = df.rename(columns={'AeropuertoKey': 'AeropuertoDestinoKey'}).drop(columns=['Nombre'])

# --- PASO 5: Carga de Tabla de Hechos (Vuelo) ---
    print("Preparando inserción masiva en tabla Vuelo (Identity delegada a SQL Server)...")
    
    # 1. Seleccionar exactamente las columnas de hechos y claves foráneas
    df_vuelo = df[[
        'TiempoKey', 'TransportistaKey', 'AeropuertoOrigenKey', 'AeropuertoDestinoKey',
        'Class', 'DepScheduled', 'DepPerformed', 'Seats', 'Passengers', 'OcupPasajeros',
        'Payload', 'Freight', 'OcupCarga', 'Distance', 'DemoraPista'
    ]].copy()

    # 2. Renombrar según el esquema físico (Excluyendo VueloKey que se auto-genera)
    df_vuelo.columns = [
        'TiempoKey', 'TransportistaKey', 'AeropuertoOrigenKey', 'AeropuertoDestinoKey',
        'tipoServicio', 'VProgramados', 'VRealizados', 'Asientos', 'Pasajeros', 'OcupPasajeros',
        'Capacidad', 'Carga', 'OcupCarga', 'Distancia', 'DemoraPista'
    ]

    print(f"Insertando {len(df_vuelo)} registros consolidados en la tabla de hechos Vuelo...")
    
    # 3. Inserción final. Al usar if_exists='append', Pandas ignora la falta de VueloKey 
    # y SQL Server la completa automáticamente.
    df_vuelo.to_sql('Vuelo', engine_dw, if_exists='append', index=False)
    
    tiempo_total = time.time() - inicio
    print("\n" + "="*60)
    print(f" CARGA INICIAL FINALIZADA EXITOSAMENTE EN {tiempo_total:.2f}s")
    print("="*60)

if __name__ == "__main__":
    carga_inicial_dw()