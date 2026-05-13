-- ===================================================== 
-- Data Warehouse - Tráfico Aéreo (RITA / DOT) 
-- Motor: SQL Server 
-- ===================================================== 

-- Crear la base de datos 
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'dw_trafico_aereo')
BEGIN 
CREATE DATABASE dw_trafico_aereo;
END 
GO 

USE dw_trafico_aereo; 
GO

-- ===================================================== 
-- DIMENSIÓN: Tiempo 
-- ===================================================== 
CREATE TABLE Tiempo ( 
TiempoKey	INT 			NOT NULL IDENTITY(1,1), 
Mes 		TINYINT 		NOT NULL, 
Estacion 	VARCHAR(20)	NOT NULL, 
Anio 		SMALLINT 		NOT NULL, 
CONSTRAINT PK_Tiempo PRIMARY KEY (TiempoKey), 
CONSTRAINT CK_Tiempo_Mes 	CHECK (Mes BETWEEN 1 AND 12),
CONSTRAINT CK_Tiempo_Anio	CHECK (Anio BETWEEN 1990 AND 2100) );
GO

-- ===================================================== 
-- DIMENSIÓN: Transportista
-- ===================================================== 
CREATE TABLE Transportista ( 
TransportistaKey	INT 			NOT NULL IDENTITY(1,1), 
CarrierNombre 	VARCHAR(150)	NOT NULL, 
CarrierGrupo 	VARCHAR(50) 	NULL, 
Region 		VARCHAR(50) 	NULL,
Fecha_Inicio	DATE			NOT NULL,
Fecha_Fin		DATE			NULL,
Fila_Activa	BIT			NOT NULL DEFAULT 1,
CONSTRAINT PK_Transportista PRIMARY KEY (TransportistaKey) 
); 
GO 

-- ===================================================== 
-- DIMENSIÓN: Aeropuerto (Origen y Destino) 
-- ===================================================== 
CREATE TABLE Aeropuerto ( 
AeropuertoKey 	INT 			NOT NULL IDENTITY(1,1), 
Nombre 		VARCHAR(150)	NOT NULL, 
Ciudad 		VARCHAR(100) 	NULL, 
Pais 			VARCHAR(100) 	NULL, 
Fecha_Inicio	DATE			NOT NULL,
Fecha_Fin		DATE			NULL,
Fila_Activa	BIT			NOT NULL DEFAULT 1,
CONSTRAINT PK_Aeropuerto PRIMARY KEY (AeropuertoKey)
); 
GO 

-- ===================================================== 
-- TABLA DE HECHOS: Vuelo 
-- Granularidad: segmento de vuelo mensual por transportista 
-- entre un aeropuerto origen y un aeropuerto destino 
-- ===================================================== 
CREATE TABLE Vuelo ( 
	VueloKey			INT 			NOT NULL IDENTITY(1,1),
TiempoKey 			INT 			NOT NULL, 
TransportistaKey 	INT 			NOT NULL, 
AeropuertoOrigenKey 	INT 			NOT NULL, 
AeropuertoDestinoKey 	INT 			NOT NULL, 
tipoServicio 		VARCHAR(50) 	NOT NULL, 
VProgramados 		INT 			NOT NULL DEFAULT 0, 
VRealizados 		INT 			NOT NULL DEFAULT 0, 
Asientos 			INT 			NOT NULL DEFAULT 0, 
Pasajeros 			INT 			NOT NULL DEFAULT 0, 
OcupPasajeros 		DECIMAL(5,4) 	NULL, 

Capacidad 			DECIMAL(18,2) 		NOT NULL DEFAULT 0, 
Carga 			DECIMAL(18,2)	NOT NULL DEFAULT 0, 
OcupCarga 			DECIMAL(5,4) 	NULL, 
Distancia 			DECIMAL(10,2)	NOT NULL DEFAULT 0, 
DemoraPista		INT			NOT NULL DEFAULT 0,

CONSTRAINT PK_Vuelo PRIMARY KEY (VueloKey), 

CONSTRAINT FK_Vuelo_Tiempo
FOREIGN KEY (TiempoKey) 
REFERENCES Tiempo(TiempoKey),
 
CONSTRAINT FK_Vuelo_Transportista 
FOREIGN KEY (TransportistaKey) 
REFERENCES Transportista(TransportistaKey), 

CONSTRAINT FK_Vuelo_AeropuertoOrigen 
FOREIGN KEY (AeropuertoOrigenKey) 
REFERENCES Aeropuerto(AeropuertoKey), 

CONSTRAINT FK_Vuelo_AeropuertoDestino 
FOREIGN KEY (AeropuertoDestinoKey) 
REFERENCES Aeropuerto(AeropuertoKey), 
); 
GO

-- ===================================================== 
-- Índices adicionales para optimizar consultas analíticas 
-- ===================================================== 
CREATE INDEX IDX_Vuelo_Tiempo ON Vuelo(TiempoKey); 
CREATE INDEX IDX_Vuelo_Transportista ON Vuelo(TransportistaKey); 
CREATE INDEX IDX_Vuelo_Origen ON Vuelo(AeropuertoOrigenKey); 
CREATE INDEX IDX_Vuelo_Destino ON Vuelo(AeropuertoDestinoKey); 
GO
