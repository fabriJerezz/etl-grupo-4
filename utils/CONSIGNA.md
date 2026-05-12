# Guía del Trabajo Práctico: Integración de Datos y Carga de Data Warehouse (DW)

## Objetivo del Trabajo
El objetivo de este trabajo es poner en práctica los conceptos fundamentales de **integración de datos, modelado multidimensional y carga de un Data Warehouse (DW)**, incluyendo consideraciones de integridad, historización y actualización incremental.

### Herramientas Sugeridas
La implementación se realizará sobre un motor de base de datos relacional (PostgreSQL, MySQL, SQL Server, Oracle, etc.) utilizando:
* **Python:** pandas, SQLAlchemy, scripts ETL.
* **Pentaho Data Integration:** Kettle.
* **Otras:** SQL puro (Stored Procedures) o herramientas equivalentes.

---

## 1. Carga Inicial del Data Warehouse (DW)
Implementación del proceso de carga masiva desde los datos operacionales hacia el modelo diseñado (hechos y dimensiones).

### Tareas Requeridas

#### A. Obtención y preparación de datos fuentes
* Proveer archivos de origen (CSV, Excel o SQL).
* Validar claves y atributos completos/consistentes.
* Detectar valores nulos, inconsistencias y duplicados.
* *Alineado con los procesos de limpieza e integración del backend del DW.*

#### B. Desarrollo de un proceso ETL inicial
El flujo debe contemplar las siguientes fases:
1.  **Staging Area:** Cargar datos en tablas intermedias para validación previa.
2.  **Transformaciones:** Limpieza, conversión de tipos, normalización y eliminación de duplicados.
3.  **Carga de Dimensiones:** Inserción jerárquica y generación de **Surrogate Keys** (claves sustitutas).
4.  **Carga de Tabla de Hechos:** Resolución de claves foráneas y coherencia dimensional.

#### C. Controles de integridad
* Validar integridad referencial entre hechos y dimensiones.
* Manejo de registros desconocidos (ej: `Surrogate Key = -1` para "Unknown").
* Control de consistencia y completitud.

#### D. Diseño de carga repetible (Idempotencia)
El proceso debe poder ejecutarse múltiples veces sin generar duplicados:
* Estrategias: `TRUNCATE` + carga completa o `MERGE / UPSERT`.
* Manejo de errores y logging de registros rechazados.

#### E. Optimización de performance (Bonus)
* Uso de índices en tablas de staging.
* Procesamiento por lotes (batch processing).
* Manejo eficiente de transacciones (commits).

---

## 2. Consideraciones de Reprocesamiento
El proceso ETL debe ser re-ejecutable sin efectos colaterales.
* **Aislamiento:** Uso de la zona de Staging.
* **Validación:** Verificaciones previas a la inserción.
* **Estrategia:** Definir claramente cuándo aplicar carga `FULL` vs `INCREMENTAL`.

---

## 3. Carga Incremental y Actualización del DW
Incorporación de nuevos datos transcurridos un período de tiempo sin reconstruir el DW completo.

### Tareas y Aspectos Clave
* **Detección de cambios:** Identificar nuevos registros y modificaciones.
* **Atributos de control:** Uso de timestamps (`LastUpdatedDate`) o hashes de cambio.
* **Optimización:** Reducción del tiempo de ejecución al procesar solo deltas.

---

## 4. Cambios en Dimensiones (SCD)
Explicar e implementar el manejo de la evolución de los datos en las dimensiones (**Slowly Changing Dimensions**).

### Tipos de SCD
* **Tipo 1:** Sobrescritura (no guarda historial).
* **Tipo 2:** Histórico completo (creación de nuevos registros con vigencia). **(Recomendado)**.
* **Tipo 3:** Valor anterior (agrega una columna para el estado previo).

> **Requisito:** Implementar al menos un caso práctico (preferentemente Tipo 2) mostrando el estado antes y después de la carga.

---

## 5. Calidad de Datos y Metadatos (Opcional)
*Puntaje adicional para el grupo.*

* **Calidad:** Reglas de limpieza y validación de formatos/nulos.
* **Metadatos:** Documentación de linaje de datos (tablas/columnas), registro de fechas de carga, volúmenes procesados y logs de errores.

---

## Entrega Final
Subir un archivo `.zip` con el nombre: `TP2-GrupoX.zip` conteniendo:

### 📁 Proyecto ETL
* Scripts Python, archivos `.ktr/.kjb` de Kettle o archivos SQL.

### 📁 Datos Fuente
* Archivos utilizados o un documento con los enlaces de descarga.

### 📄 Documento PDF Explicativo
Debe incluir:
1.  **Estrategia:** Descripción de carga inicial e incremental.
2.  **Manejo de SCD:** Detalles de la implementación elegida.
3.  **Flujo ETL:** Diagrama del proceso y principales transformaciones.
4.  **Evidencias:** Capturas de pantalla o resultados de queries.
5.  **Análisis:** Consideraciones de calidad y estructura de datos.