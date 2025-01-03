'''import pandas as pd
import numpy as np
from Utils.utils import cargar_datos_carpeta, combinar_datos

# Cargar datos existentes
raw_path = '../data/raw/'
dataframe_dict = cargar_datos_carpeta(raw_path)
df_raw = combinar_datos(dataframe_dict)

df_melted = df_raw.melt(id_vars=['nombre','id_partido'], value_vars=['equipo_local', 'equipo_visitante'], var_name='tipo_equipo', value_name='equipo')

# Eliminar duplicados para contar partidos únicos por jugador y equipo
df_unicos = df_melted.drop_duplicates(subset=['nombre', 'equipo', 'id_partido'])
# Contar el número de partidos únicos por jugador y equipo
conteo_partidos = df_unicos.groupby(['nombre', 'equipo'])['id_partido'].count().reset_index(name='num_partidos')
# Obtener el equipo más frecuente para cada jugador
idx_max = conteo_partidos.groupby('nombre')['num_partidos'].idxmax()
resultado = conteo_partidos.loc[idx_max]
# Asignar NaN a los jugadores que solo jugaron un partido
resultado.loc[resultado['num_partidos'] == 1, 'equipo'] = None
# Mostrar solo las columnas relevantes
resultado[resultado['equipo'].isnull()]
jugador_equipo = resultado.loc[:,['nombre','equipo']]

df_con_equipo = pd.merge(df_raw, resultado[['nombre', 'equipo']], on='nombre', how='left')

# Transformacion lineal de las coordenadas de los tiros. 
df_con_equipo['coord_x']=df_con_equipo['x']
df_con_equipo['coord_y']=df_con_equipo['y']

# Definir las condiciones
condiciones = [
    df_con_equipo['equipo'] == df_con_equipo['equipo_local'],       # Primera condición
    df_con_equipo['equipo'] == df_con_equipo['equipo_visitante'],   # Segunda condición
    df_con_equipo['equipo'].isnull()                               # Tercera condición: el equipo es NaN
]

# Definir los valores correspondientes para cada condición
valores_x = [
    (df_con_equipo['coord_y'] - 141.25) * 6,                       # Si es el equipo local
    -(df_con_equipo['coord_y'] - 141.25) * 6,                      # Si es el equipo visitante
    None                                                           # Si no se conoce el equipo
]

# Definir los valores correspondientes para cada condición
valores_y = [
    (df_con_equipo['coord_x'] - 24.750) * 6,                        # Si es el equipo local
    abs((df_con_equipo['coord_x'] - 24.750) * 6 - 2800),            # Si es el equipo visitante
    None                                                            # Si no se conoce el equipo
]


# Aplicar las condiciones con np.select
df_con_equipo['x'] = np.select(condiciones, valores_x, default=np.nan)  # Default en caso de no cumplir ninguna condición
df_con_equipo['y'] = np.select(condiciones, valores_y, default=np.nan)  # Default en caso de no cumplir ninguna condición

df_con_equipo.drop(labels=['coord_x','coord_y'],
           axis=1,
           inplace=True)'''

import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

class LanzamientosProcesadosManager:
    def __init__(self):
        load_dotenv()
        self.connection = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

    def create_or_update_table(self):
        """Crea o asegura que la tabla presentation.lanzamientos exista."""
        create_table_query = """
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
        """
        with self.connection.cursor() as cursor:
            cursor.execute(create_table_query)
            self.connection.commit()
            print("Tabla presentation.lanzamientos_procesados creada o verificada correctamente.")

    def update_table_data(self):
        """Actualiza la tabla presentation.lanzamientos con nuevos datos."""
        update_query = """
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
        """
        with self.connection.cursor() as cursor:
            cursor.execute(update_query)
            self.connection.commit()
            print("Datos actualizados en presentation.lanzamientos.")

    def close_connection(self):
        self.connection.close()

if __name__ == "__main__":
    manager = LanzamientosProcesadosManager()
    try:
        manager.create_or_update_table()
        manager.update_table_data()
    finally:
        manager.close_connection()
