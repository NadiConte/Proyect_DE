-- Tabla Dim_Tiempo
CREATE TABLE IF NOT EXISTS Dim_Tiempo (
    id_tiempo DATE PRIMARY KEY,
    anio INT,
    mes INT,
    dia INT
);

-- Tabla Dim_Peliculas
CREATE TABLE IF NOT EXISTS Dim_Peliculas (
    id_pelicula INT PRIMARY KEY,
    titulo VARCHAR(255),
    anio INT,
    puntuacion FLOAT,
    duracion INT,
    idioma_original VARCHAR(255),
    presupuesto FLOAT
);

-- Tabla Dim_Directores
CREATE TABLE IF NOT EXISTS Dim_Directores (
    id_director INT PRIMARY KEY,
    nombre VARCHAR(255),
    fecha_nacimiento DATE,
    nacionalidad VARCHAR(255)
);

-- Tabla Fact_Table
CREATE TABLE IF NOT EXISTS Fact_Table (
    id_pelicula INT,
    id_director INT,
    id_tiempo DATE,
    prom_puntuacion FLOAT,
    cant_votos INT,
    popularidad FLOAT,
    recaudacion FLOAT,
    PRIMARY KEY (id_pelicula, id_director, id_tiempo)
);