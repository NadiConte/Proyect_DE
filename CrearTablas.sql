-- Tabla Dim_Tiempo
CREATE TABLE Dim_Tiempo (
    id_tiempo DATE PRIMARY KEY,
    anio INT,
    mes INT,
    dia INT
);

-- Tabla Dim_Actores
CREATE TABLE Dim_Actores (
    id_actor INT PRIMARY KEY,
    nombre VARCHAR(255)
);

-- Tabla Dim_Peliculas
CREATE TABLE Dim_Peliculas (
    id_pelicula INT PRIMARY KEY,
    titulo VARCHAR(255),
    anio INT,
    duracion INT,
    plataforma VARCHAR(255),
    idioma_original VARCHAR(255),
    clasificacion VARCHAR(10),
    puntuacion FLOAT
);

-- Tabla Dim_Directores
CREATE TABLE Dim_Directores (
    id_director INT PRIMARY KEY,
    nombre VARCHAR(255),
    fecha_nacimiento DATE,
    nacionalidad VARCHAR(255)
);

-- Tabla Fact_Table
CREATE TABLE Fact_Table (
    id_pelicula INT,
    id_director INT,
    id_actor INT,
    id_tiempo DATE,
    prom_puntuacion FLOAT
);