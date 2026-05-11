import os
import time
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# =============================================================
# CONFIGURACIÓN
# =============================================================
ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(ENV_PATH)

CONNECTION_STRING = os.getenv("DB_CONNECTION_STRING")

# =============================================================
# TRANSFORMACIONES (Reglas 14, 15 y 16)
# =============================================================

def transformar_datos(engine):
    print("=" * 60)
    print("ETL - TRANSFORMACIÓN Y LIMPIEZA DE DATOS")
    print("=" * 60)

    queries = [
        # -------------------------------------------------------------
        # Regla 14: Seats = 0 con Passengers > 0
        # "Descartar: Passengers > Seats (salvo Seats = 0 → flag para revisión)"
        # -------------------------------------------------------------
        (
            "Agregando flag Flag_Review_Seats...",
            """
            IF COL_LENGTH('dbo.stg_t100', 'Flag_Review_Seats') IS NULL
            BEGIN
                ALTER TABLE dbo.stg_t100 ADD Flag_Review_Seats INT DEFAULT 0;
            END
            """
        ),
        (
            "Marcando registros con Seats = 0 y Passengers > 0...",
            """
            UPDATE dbo.stg_t100 
            SET Flag_Review_Seats = 1 
            WHERE CAST(Seats AS INT) = 0 AND CAST(Passengers AS INT) > 0;
            """
        ),
        (
            "Descartando registros donde Passengers > Seats y Seats > 0...",
            """
            DELETE FROM dbo.stg_t100 
            WHERE CAST(Passengers AS INT) > CAST(Seats AS INT) AND CAST(Seats AS INT) > 0;
            """
        ),

        # -------------------------------------------------------------
        # Regla 15: Freight negativo
        # "Descartar: Freight > Payload OR Freight < 0"
        # -------------------------------------------------------------
        (
            "Descartando registros con Freight negativo...",
            """
            DELETE FROM dbo.stg_t100 
            WHERE CAST(Freight AS FLOAT) < 0;
            """
        ),

        # -------------------------------------------------------------
        # Regla 16: Carriers inactivos con datos en 2023
        # "Marcar carriers inactivos con flag IsActive = 0 cruzando con l_carrier_history"
        # -------------------------------------------------------------
        (
            "Agregando flag IsActive...",
            """
            IF COL_LENGTH('dbo.stg_t100', 'IsActive') IS NULL
            BEGIN
                ALTER TABLE dbo.stg_t100 ADD IsActive INT DEFAULT 1;
            END
            """
        ),
        (
            "Seteando IsActive por default a 1...",
            """
            UPDATE dbo.stg_t100 SET IsActive = 1 WHERE IsActive IS NULL;
            """
        ),
        (
            "Marcando carriers inactivos...",
            """
            UPDATE t
            SET t.IsActive = 0
            FROM dbo.stg_t100 t
            INNER JOIN dbo.stg_carrier_history h 
                ON t.AirlineID = h.AirlineID 
               AND t.UniqueCarrier = h.UniqueCarrier
            WHERE h.EndDate IS NOT NULL 
              AND LTRIM(RTRIM(h.EndDate)) <> ''
              AND TRY_CAST(h.EndDate AS DATE) < '2023-01-01';
            """
        )
    ]

    inicio_total = time.time()
    
    with engine.begin() as conn:
        for descripcion, query in queries:
            print(f"-> {descripcion}")
            inicio_query = time.time()
            result = conn.execute(text(query))
            
            # Solo mostrar filas afectadas si es UPDATE o DELETE
            if query.strip().upper().startswith(("UPDATE", "DELETE")):
                print(f"   Filas afectadas: {result.rowcount:,}")
                
            elapsed = time.time() - inicio_query
            print(f"   Tiempo: {elapsed:.2f}s")

    elapsed_total = time.time() - inicio_total
    print("=" * 60)
    print(f"TRANSFORMACIONES COMPLETADAS EN {elapsed_total:.2f}s")
    print("=" * 60)


if __name__ == "__main__":
    if not CONNECTION_STRING:
        print("ERROR: No se encontró DB_CONNECTION_STRING en el .env")
        exit(1)

    print("Conectando a SQL Server...")
    engine = create_engine(CONNECTION_STRING)
    
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("[OK] Conexión exitosa")
    except Exception as e:
        print(f"X ERROR de conexión: {e}")
        exit(1)

    transformar_datos(engine)
