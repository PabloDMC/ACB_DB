import pandas as pd
from Utils.data_processing import corregir_nombre_ambiguo, actualizar_dataframe
import argparse
import pickle

def procesar_jornada(jornada,competicion,temporada):
    """
    Actualización de las tablas 'jornadas','partidos' y 'tiros' de la base de datos.
    """
    # Lectura de los datos necesarios para la creación y actualización de las tablas.
    with open("data/processed/jugadores_equipos.pkl", "rb") as f:
        diccionario_df = pickle.load(f)
    with open("data/processed/static_data.pkl", "rb") as f:
        diccionario_static_df = pickle.load(f)
    with open("data/processed/dynamic_data.pkl", "rb") as f:
        diccionario_dynamic_df = pickle.load(f)
    jugadores = diccionario_df['jugadores']
    jugadores_equipos = diccionario_df['jugadores_equipos']
    equipos = diccionario_static_df['equipos']
    df_jornada = diccionario_dynamic_df['jornadas']
    df_partidos = diccionario_dynamic_df['partidos']
    df_tiros = diccionario_dynamic_df['tiros']
    df_raw = pd.read_csv(f'data/raw/new/jornada_{jornada}_{competicion}_{temporada}.csv')
    
    # Corrección del nombre de equipos.
    equipos_distintos = {'Betis': 'Real Betis Baloncesto',
                        'Bitci Baskonia': 'Baskonia',
                        'Dreamland GC': 'Dreamland Gran Canaria',
                        'Herbalife GC': 'Herbalife Gran Canaria',
                        'Hereda SPB': 'Hereda San Pablo Burgos',
                        'Joventut': 'Club Joventut Badalona',
                        'Movistar Estu': 'Movistar Estudiantes',
                        'RETAbet Bilbao': 'RETAbet Bilbao Basket'}
    df_raw.replace({'equipo_local':equipos_distintos},inplace=True)
    df_raw.replace({'equipo_visitante':equipos_distintos},inplace=True)
    
    # Actualización de la tabla jornadas.
    jornadas_nuevo = df_raw[['jornada','temporada','competicion','playoff']] \
       .drop_duplicates() \
       .replace({'temporada':{'2020-2021':1,
                              '2021-2022':2,
                              '2022-2023':3,
                              '2023-2024':4,
                              '2024-2025':5},
                 'competicion':{'Liga Endesa':1,
                                'Copa del Rey':2,
                                'Supercopa Endesa':3}}) \
       .reset_index(drop=True) \
       .rename(columns = {'index':'id_jornada',
                          'jornada':'numero_jornada',
                          'temporada':'id_temporada',
                          'competicion':'id_competicion'})
    jornadas = actualizar_dataframe(df_jornada,jornadas_nuevo,'id_jornada')
    
    # Actualización de la tabla partidos.
    partidos_nuevo = df_raw \
        .loc[:,['id_partido','equipo_local','resultado_local','resultado_visitante','jornada','equipo_visitante','temporada','competicion']] \
        .groupby(['id_partido'], as_index=False) \
        .max() \
        .replace({'temporada':{'2020-2021':1,'2021-2022':2,'2022-2023':3,'2023-2024':4,'2024-2025':5},
                  'competicion':{'Liga Endesa':1,'Copa del Rey':2,'Supercopa Endesa':3}}) \
        .rename({'competicion':'id_competicion','jornada':'numero_jornada','temporada':'id_temporada'},axis=1)
    partidos_nuevo = pd.merge(partidos_nuevo,
                        equipos[['id_equipo','nombre_equipo','id_temporada']],
                        left_on = ['equipo_local','id_temporada'],
                        right_on = ['nombre_equipo','id_temporada'],
                        how = 'left') \
                .rename(columns={'id_equipo':'id_equipo_local'}) \
                .drop('nombre_equipo',axis=1) # Unión para incluir id_equipo_local
    partidos_nuevo = pd.merge(partidos_nuevo,
                        equipos[['id_equipo','nombre_equipo','id_temporada']],
                        left_on = ['equipo_visitante','id_temporada'],
                        right_on = ['nombre_equipo','id_temporada'],
                        how = 'left') \
                .rename(columns={'id_equipo':'id_equipo_visitante'}) \
                .drop(['nombre_equipo','equipo_local','equipo_visitante'],axis=1) # Unión para incluir id_equipo_visitante
    partidos_nuevo = pd.merge(partidos_nuevo,
                        jornadas[['id_jornada','numero_jornada','id_temporada','id_competicion']],
                        on = ['numero_jornada','id_temporada','id_competicion'],
                        how = 'left') \
                .drop(['numero_jornada','id_temporada','id_competicion'],axis=1) # Unión para incluir id_jornada
    partidos = pd.concat([df_partidos,partidos_nuevo],ignore_index=True).drop_duplicates() # Actualización
    
    # Actualización de la tabla tiros.
    df_tiros_sin_id = df_raw.drop(['jornada','competicion','playoff'],axis=1) \
                        .replace({'temporada':{'2020-2021':1,
                                               '2021-2022':2,
                                               '2022-2023':3,
                                               '2023-2024':4,
                                               '2024-2025':5}}) \
                        .rename({'nombre':'nombre_jugador',
                                 'temporada':'id_temporada'},axis=1)
    df_tiros_sin_id = pd.merge(df_tiros_sin_id,
                        equipos[['id_equipo','nombre_equipo','id_temporada']],
                        left_on = ['equipo_local','id_temporada'],
                        right_on = ['nombre_equipo','id_temporada'],
                        how = 'left') \
                .rename({'id_equipo':'id_local'},axis=1) \
                .drop('nombre_equipo',axis=1)
    df_tiros_sin_id = pd.merge(df_tiros_sin_id,
                        equipos[['id_equipo','nombre_equipo','id_temporada']],
                        left_on = ['equipo_visitante','id_temporada'],
                        right_on = ['nombre_equipo','id_temporada'],
                        how = 'left') \
                .rename({'id_equipo':'id_visitante'},axis=1) \
                .drop(['nombre_equipo','equipo_local','equipo_visitante'],axis=1)
    # Existen jugadores distintos que comparten un mismo nombre debido al formato del nombre (Ej. Mario Pérez y Marcos Pérez --> M. Pérez).
    # Para evitar conflictos a la hora de hacer la unión para incluir id_jugador en la tabla tiros, se divide la muestra en jugadores que tienen ambigüedad en su nombre y jugadores que no.
    df_nombres = jugadores_equipos.merge(jugadores).drop('id_jugador_equipo',axis=1)
    duplicados = df_nombres.groupby(['nombre_jugador', 'id_temporada'])['id_jugador'].nunique()
    duplicados = duplicados[duplicados > 1].reset_index()
    df_nombres = df_nombres.merge(duplicados, on=['nombre_jugador', 'id_temporada'],how='left') \
                            .rename(columns = {'id_jugador_x':'id_jugador',
                                               'id_jugador_y':'nombre_ambiguo'})
    df_nombres['nombre_ambiguo'] = df_nombres['nombre_ambiguo'].notna()
    df_nombre_no_ambiguo = df_nombres[df_nombres['nombre_ambiguo']==False].loc[:,['id_jugador','nombre_jugador','id_temporada']] # Jugadores sin ambigüedad en su nombre.
    df_nombre_ambiguo = df_nombres[df_nombres['nombre_ambiguo']].loc[:,['id_jugador','nombre_jugador','id_equipo','id_temporada']] # Jugadores con ambigüedad en su nombre.
    
    tiros_nuevo = pd.merge(df_tiros_sin_id,
                   df_nombre_no_ambiguo,
                   on=['nombre_jugador','id_temporada'],
                   how='left') \
            .drop_duplicates() # Unión de los nuevos tiros para incluir el id_jugador de los jugadores sin ambigüedad.

    # Jugadores distintos con mismo nombre que coincidieron en una misma temporada.
    estructura = {}
    for _, row in df_nombre_ambiguo.iterrows():
        clave = (row['nombre_jugador'], row['id_temporada'])
        if clave not in estructura:
            estructura[clave] = []
        estructura[clave].append((row['id_jugador'], row['id_equipo']))
    
    tiros_nuevo['id_jugador'] = tiros_nuevo.apply(corregir_nombre_ambiguo, axis=1, args=(estructura,)) # Se asigna el id_jugador correspondiente a lanzamientos con ambigüedad.
    tiros_nuevo = tiros_nuevo \
        .drop(['nombre_jugador','id_local','id_visitante','id_temporada'],axis=1) \
        .reset_index(drop=True) \
        .rename({'resultado_local':'puntos_local',
                 'resultado_visitante':'puntos_visitante'},axis=1) 
    tiros_nuevo = tiros_nuevo[['id_jugador','x','y','descripcion','anotado','cuarto','tiempo','id_partido','puntos_local','puntos_visitante']] # Columnas de tiros reordenadas.
    tiros = actualizar_dataframe(df_tiros,tiros_nuevo,'id_tiro') # Actualización de la tabla tiros.
    
    # Se guardan las tablas en un diccionario de dataframes en un archivo pickle.
    dataframes = {
        "jornadas": jornadas,
        "partidos": partidos,
        "tiros": tiros
    }
    with open("data/processed/dynamic_data.pkl", "wb") as f:
        pickle.dump(dataframes, f)
    print('Tablas de jornadas, partidos y tiros actualizadas.')
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Procesado de los datos de una jornada específica.")
    parser.add_argument("-j", "--jornada", type=int, required=True, help="Número de la jornada.")
    parser.add_argument("-c", "--competicion", type=str, required=True, help="ID de la competicion.")
    parser.add_argument("-t", "--temporada", type=int, required=True, help="Año de inicio de la temporada.")
    args = parser.parse_args()

    procesar_jornada(args.jornada, args.competicion, args.temporada)