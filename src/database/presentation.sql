CREATE SCHEMA IF NOT EXISTS presentation;

CREATE TABLE IF NOT EXISTS presentation.lanzamientos (
    id_tiro SERIAL PRIMARY KEY,
    id_jugador INT NOT NULL,
    nombre_jugador VARCHAR(100),
    equipo_jugador VARCHAR(100),
    id_equipo_jugador INT,
    x FLOAT,
    y FLOAT,
    descripcion TEXT,
    anotado BOOLEAN,
    cuarto VARCHAR(3),
    tiempo VARCHAR(10),
    puntos_local INT,
    puntos_visitante INT,
    equipo_local VARCHAR(100),
    id_equipo_local INT,
    equipo_visitante VARCHAR(100),
    id_equipo_visitante INT,
    id_partido INT NOT NULL,
    numero_jornada INT,
    temporada VARCHAR(20),
    competicion VARCHAR(50)
);


INSERT INTO presentation.lanzamientos (
    id_tiro, id_jugador, nombre_jugador, equipo_jugador, id_equipo_jugador, x, y, descripcion, anotado, cuarto, tiempo, 
    puntos_local, puntos_visitante, equipo_local, id_equipo_local, equipo_visitante, id_equipo_visitante, 
    id_partido, numero_jornada, temporada, competicion
)
SELECT 
    t.id_tiro, t.id_jugador, j.nombre_jugador AS nombre_jugador, ej.nombre_equipo AS equipo_jugador, 
    ej.id_club AS id_equipo_jugador, t.x, t.y, t.descripcion, 
    t.anotado, t.cuarto, t.tiempo, t.puntos_local, t.puntos_visitante, 
    el.nombre_equipo AS equipo_local, el.id_club AS id_equipo_local, ev.nombre_equipo AS equipo_visitante, 
    ev.id_club AS id_equipo_visitante, p.id_partido, jr.numero_jornada, 
    CONCAT(te.anio_inicio, '-', te.anio_fin) AS temporada, c.nombre_competicion AS competicion
FROM tiros t
JOIN jugadores j ON t.id_jugador = j.id_jugador
JOIN partidos p ON t.id_partido = p.id_partido
JOIN jornadas jr ON p.id_jornada = jr.id_jornada
JOIN temporadas te ON jr.id_temporada = te.id_temporada
JOIN competiciones c ON jr.id_competicion = c.id_competicion
JOIN equipos el ON p.id_equipo_local = el.id_equipo
JOIN equipos ev ON p.id_equipo_visitante = ev.id_equipo
LEFT JOIN (
    SELECT 
        id_jugador, 
        id_temporada, 
        MAX(id_equipo) AS id_equipo
    FROM 
        jugadores_equipos
    GROUP BY 
        id_jugador, id_temporada
) je ON t.id_jugador = je.id_jugador AND jr.id_temporada = je.id_temporada
LEFT JOIN equipos ej ON je.id_equipo = ej.id_equipo
ON CONFLICT DO NOTHING;