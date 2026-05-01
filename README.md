# etl-grupo-4

Proyecto ETL para cargar archivos CSV a un staging en SQL Server.

## Requisitos

- Python 3.x
- Driver ODBC para SQL Server (ODBC Driver 17 o 18)
	- Descarga: https://learn.microsoft.com/es-es/sql/connect/odbc/download-odbc-driver-for-sql-server

## Entorno virtual (Windows)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
```

## Dependencias

```powershell
pip install -r requirements.txt
```

Si necesitas actualizar el archivo:

```powershell
pip freeze > requirements.txt
```

## Configuracion

1) Copiar el ejemplo y completar los datos reales:

```powershell
copy .env.example .env
```

2) Ajustar la variable `DB_CONNECTION_STRING` en `.env`.

Ejemplo (autenticacion Windows):

```env
DB_CONNECTION_STRING=mssql+pyodbc://@localhost/dw_staging_raw?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes
```

Ejemplo (autenticacion SQL):

```env
DB_CONNECTION_STRING=mssql+pyodbc://usuario:password@localhost/dw_staging_raw?driver=ODBC+Driver+17+for+SQL+Server
```

Notas:
- Verifica que el driver ODBC instalado coincida con el nombre usado en la cadena.
- Si usas ODBC Driver 18, cambia el nombre en la cadena de conexion.
- Si usas una instancia con nombre, usa `localhost%5CINSTANCIA` o un puerto fijo `localhost,PUERTO`.

## Preparar base de datos

Ejecutar el script en utils/crear_staging.md desde SSMS. Crea la base
dw_staging_raw y todas las tablas stg_ (re-ejecutable).

## Ejecucion

```powershell
python extract.py
```

## Estado del proyecto

- extract.py implementado.
- load.py y transform.py pendientes.

## Documentacion

- Ver docs/ para documentacion especifica de cada etapa.