DROP TABLE IF EXISTS jugadores CASCADE;
DROP TABLE IF EXISTS clubes CASCADE;
DROP TABLE IF EXISTS competiciones CASCADE;
DROP TABLE IF EXISTS temporadas CASCADE;
DROP TABLE IF EXISTS equipos CASCADE; 
DROP TABLE IF EXISTS jornadas CASCADE;
DROP TABLE IF EXISTS partidos CASCADE;
DROP TABLE IF EXISTS tiros CASCADE;
DROP TABLE IF EXISTS jugadores_equipos CASCADE;

-- Tabla de jugadores
CREATE TABLE jugadores (
    id_jugador INT PRIMARY KEY,
    nombre_jugador VARCHAR(50) NOT NULL
    --fecha_nacimiento DATE
    --nacionalidad VARCHAR(100)
);

-- Tabla de clubes
CREATE TABLE clubes (
    id_club INT PRIMARY KEY,
    nombre_oficial VARCHAR(50) NOT NULL
);

-- Tabla de competencias
CREATE TABLE competiciones (
    id_competicion SERIAL PRIMARY KEY,
    nombre_competicion VARCHAR(20) NOT NULL
);

-- Tabla de temporadas
CREATE TABLE temporadas (
    id_temporada SERIAL PRIMARY KEY,
    anio_inicio INT NOT NULL,
    anio_fin INT NOT NULL,
    temporada CHAR(9) NOT NULL
);

-- Tabla de equipos
CREATE TABLE equipos (
    id_equipo SERIAL PRIMARY KEY,
    id_club INT REFERENCES clubes(id_club),
    nombre_equipo VARCHAR(30) NOT NULL,
    id_temporada INT REFERENCES temporadas(id_temporada)
    --nombre_patrocinador VARCHAR(255)
);

-- Tabla de jornadas
CREATE TABLE jornadas (
    id_jornada SERIAL PRIMARY KEY,
    numero_jornada INT NOT NULL,
    id_temporada INT REFERENCES temporadas(id_temporada),
    id_competicion INT REFERENCES competiciones(id_competicion),
    playoff BOOLEAN
);

-- Tabla de partidos
CREATE TABLE partidos (
    id_partido INT PRIMARY KEY,
    resultado_local INT,
    resultado_visitante INT,
    id_equipo_local INT REFERENCES equipos(id_equipo),
    id_equipo_visitante INT REFERENCES equipos(id_equipo),
    id_jornada INT REFERENCES jornadas(id_jornada)
    -- fecha_partido DATE NOT NULL
);

-- Tabla de tiros
CREATE TABLE tiros (
    id_tiro SERIAL PRIMARY KEY,
    id_jugador INT REFERENCES jugadores(id_jugador),
    x FLOAT,
    y FLOAT,
    descripcion VARCHAR(10),
    anotado BOOLEAN,
    cuarto VARCHAR(3),
    tiempo CHAR(5),
    id_partido INT REFERENCES partidos(id_partido),
    puntos_local INT,
    puntos_visitante INT
);

-- Tabla de historial de equipos por jugador
CREATE TABLE jugadores_equipos (
    id_jugador_equipo SERIAL PRIMARY KEY,
    id_jugador INT REFERENCES jugadores(id_jugador),
    id_equipo INT REFERENCES equipos(id_equipo),
    id_temporada INT REFERENCES temporadas(id_temporada)
    --fecha_inicio DATE,
    --fecha_fin DATE
);
