import time
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import extract
import transform
import load

# =============================================================
# 1. CONFIGURACIÓN Y CONEXIONES
# =============================================================
ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(ENV_PATH)

CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")
engine_stg = create_engine(CONNECTION_STRING, fast_executemany=True)

def ejecutar_pipeline_etl():
    print("=" * 60)
    print(" INICIANDO PIPELINE ETL - DATA WAREHOUSE TRÁFICO AÉREO - GRUPO 4")
    print("=" * 60)
    
    start_time = time.time()

    try:
        # 1. FASE DE EXTRACCIÓN (E)
        print("\n[Fase 1/3] EXTRACCIÓN...")
        extract.ejecutar_extraccion()
        
        print("  > Extrayendo datos crudos desde Staging Area...")
        df_staging = pd.read_sql("SELECT * FROM stg_t100", engine_stg)

        # 2. FASE DE TRANSFORMACIÓN (T)
        print("\n[Fase 2/3] TRANSFORMACIÓN Y LIMPIEZA...")
        # Le pasamos el dataframe crudo al módulo de transformación
        df_limpio = transform.ejecutar_pipeline_transformacion(df_staging, engine_stg)
        
        # 3. FASE DE CARGA (L)
        print("\n[Fase 3/3] CARGA EN DATA WAREHOUSE...")
        # Le pasamos el dataframe limpio para que popule el modelo en estrella
        load.carga_inicial_dw(df_limpio)
        
        end_time = time.time()
        duracion = round(end_time - start_time, 2)
        
        print("\n" + "=" * 60)
        print(f" PIPELINE EJECUTADO CON ÉXITO. Tiempo total: {duracion} segundos.")
        print("=" * 60)

    except Exception as e:
        print("\n" + "!" * 60)
        print(" ERROR CRÍTICO EN EL PIPELINE ETL ABORTANDO OPERACIÓN.")
        print(f" Detalle del error: {str(e)}")
        print("!" * 60)

if __name__ == "__main__":
    ejecutar_pipeline_etl()