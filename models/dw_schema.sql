-- DIM_TIEMPO
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_tiempo' AND xtype='U')
CREATE TABLE dim_tiempo (
    id_tiempo    INT IDENTITY(1,1) PRIMARY KEY,
    fecha        DATE NOT NULL UNIQUE,
    anio         INT,
    mes          INT,
    dia          INT,
    nombre_mes   VARCHAR(20),
    trimestre    INT,
    dia_semana   VARCHAR(20)
);

-- DIM_PRODUCTO
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_producto' AND xtype='U')
CREATE TABLE dim_producto (
    id_producto_dw  INT IDENTITY(1,1) PRIMARY KEY,
    id_producto_src VARCHAR(50) NOT NULL UNIQUE,
    nombre_producto VARCHAR(200),
    categoria       VARCHAR(100)
);

-- DIM_CLIENTE
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_cliente' AND xtype='U')
CREATE TABLE dim_cliente (
    id_cliente_dw  INT IDENTITY(1,1) PRIMARY KEY,
    id_cliente_src VARCHAR(100) NOT NULL UNIQUE,
    tipo_cliente   VARCHAR(50)
);

-- DIM_FUENTE
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_fuente' AND xtype='U')
CREATE TABLE dim_fuente (
    id_fuente     INT IDENTITY(1,1) PRIMARY KEY,
    nombre_fuente VARCHAR(100) NOT NULL UNIQUE,
    tipo_fuente   VARCHAR(50)
);

-- DIM_SENTIMIENTO
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_sentimiento' AND xtype='U')
CREATE TABLE dim_sentimiento (
    id_sentimiento INT IDENTITY(1,1) PRIMARY KEY,
    clasificacion  VARCHAR(50) NOT NULL UNIQUE,
    polaridad      VARCHAR(20)
);

-- FACT_OPINION
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fact_opinion' AND xtype='U')
CREATE TABLE fact_opinion (
    id_opinion        INT IDENTITY(1,1) PRIMARY KEY,
    id_tiempo         INT REFERENCES dim_tiempo(id_tiempo),
    id_producto       INT REFERENCES dim_producto(id_producto_dw),
    id_cliente        INT REFERENCES dim_cliente(id_cliente_dw),
    id_fuente         INT REFERENCES dim_fuente(id_fuente),
    id_sentimiento    INT REFERENCES dim_sentimiento(id_sentimiento),
    calificacion      FLOAT,
    score_sentimiento FLOAT,
    comentario        VARCHAR(MAX),
    id_externo        VARCHA