import pandas as pd
from Utils.data_processing import corregir_nombre_ambiguo
from Utils.data_processing import transformar_nombre,corregir_nombre_ambiguo

def procesar_raw(df_raw):
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
    
    return df_raw

def crear_tabla_jugadores(df_jugadores,equipos):
    """Crea una tabla de jugadores y otra de jugadores_equipos"""
    
    # Corrección del nombre de jugadores y equipos cuyo formato en la página https://jv.acb.com/ no coincide con el estándar.
    df_jugadores.replace({'nombre_equipo':{'Valencia Basket Club':'Valencia Basket'}},inplace=True)
    df_jugadores["nombre_jugador"] = df_jugadores['nombre_jugador'].apply(transformar_nombre)
    jugadores_distintos = {'O. Balcerowski': 'A. Balcerowski',
                        'A. Sola': 'A. Solá',
                        'M. Abdur-Rahkman': 'Abdur-Rahkman',
                        'R. Andronikashvili': 'Andronikashvili',
                        'D. Hilliard': 'D. Hilliard II',
                        'D. Jelínek': 'D. Jelinek',
                        'DJ Seeley': 'D. Seeley',
                        'DJ Strawberry': 'D. Strawberry',
                        'D. Milosavljevic': 'D.Milosavljevic',
                        'E. John Ndiaye': 'E. Ndiaye',
                        'G. Conditt IV': 'G. Conditt',
                        'N. Varela': 'I. Varela',
                        'J. Manuel Vaquero': 'J. Vaquero',
                        'J. Webb III': 'J. Webb',
                        'J. Juan Barea': 'JJ Barea',
                        'S. Killeya-Jones': 'Killeya-Jones',
                        'T. Luwawu-Cabarrot': 'Luwawu-Cabarrot',
                        'X. López-Arostegui': 'López-Arostegui',
                        'M. Diagne': 'M. Diagné',
                        'C. Miller-McIntyre': 'Miller-McIntyre',
                        'N. Rogkavopoulos': 'N.Rogkavopoulos',
                        'N. Williams-Goss': 'N.Williams-Goss',
                        'R. López De La Torre': 'R. López',
                        'TJ Cline JR': 'TJ Cline',
                        'K. Van der Vuurst': 'Van der Vuurst',
                        'E. Tavares': 'W. Tavares',
                        'A. Corpas': 'Á. Corpas',
                        'A. Delgado': 'Á. Delgado',
                        'A. Infante': 'Á. Infante',
                        'A. Muñoz': 'Á. Muñoz',
                        'I. Betolaza': 'Í. Betolaza',
                        'D. Dae Grant':'D. Grant',
                        'DJ Stephens': 'D.J. Stephens'}
    df_jugadores.replace({'nombre_jugador':jugadores_distintos},inplace=True)

    df_jugadores_equipos = df_jugadores \
                        .replace({'id_temporada':{2020:1,
                                                  2021:2,
                                                  2022:3,
                                                  2023:4,
                                                  2024:5}}) \
                        .loc[:,['id_jugador','id_equipo','nombre_equipo','id_temporada']] \
                        .merge(equipos.loc[:,['id_equipo','nombre_equipo','id_temporada']],
                            on = ['nombre_equipo','id_temporada'],
                            how = 'left') \
                        .drop(['id_equipo_x','nombre_equipo'],axis=1) \
                        .reset_index(drop=True) \
                        .rename(columns={'id_equipo_y':'id_equipo'})
    df_jugadores_equipos = df_jugadores_equipos[['id_jugador','id_equipo','id_temporada']]
    df_jugadores = df_jugadores.loc[:,['id_jugador','nombre_jugador']].sort_values(by='id_jugador').drop_duplicates()
    
    return df_jugadores, df_jugadores_equipos

def crear_tabla_jornadas(df_raw):
    """Crea una tabla de jornadas a partir de datos crudos."""
    
    df_jornadas = df_raw[['jornada','temporada','competicion','playoff']] \
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
    return df_jornadas

def crear_tabla_partidos(df_raw, jornadas, equipos):
    """Crea una tabla de partidos."""
    
    df_raw = procesar_raw(df_raw)
    partidos = df_raw \
        .loc[:,['id_partido','equipo_local','resultado_local','resultado_visitante','jornada','equipo_visitante','temporada','competicion']] \
        .groupby(['id_partido'], as_index=False) \
        .max() \
        .replace({'temporada':{'2020-2021':1,'2021-2022':2,'2022-2023':3,'2023-2024':4,'2024-2025':5},
                  'competicion':{'Liga Endesa':1,'Copa del Rey':2,'Supercopa Endesa':3}}) \
        .rename({'competicion':'id_competicion','jornada':'numero_jornada','temporada':'id_temporada'},axis=1)

    # Enlazar con id_jornada e id_equipo local y visitante
    partidos = pd.merge(partidos, equipos[['id_equipo', 'nombre_equipo', 'id_temporada']],
                        left_on = ['equipo_local', 'id_temporada'],
                        right_on = ['nombre_equipo', 'id_temporada'],
                        how = 'left') \
        .rename(columns={'id_equipo': 'id_equipo_local'}) \
        .drop('nombre_equipo', axis=1) # Unión para incluir id_equipo_local
    partidos = pd.merge(partidos, equipos[['id_equipo', 'nombre_equipo', 'id_temporada']],
                        left_on = ['equipo_visitante', 'id_temporada'],
                        right_on = ['nombre_equipo', 'id_temporada'],
                        how = 'left') \
        .rename(columns={'id_equipo': 'id_equipo_visitante'}) \
        .drop(['nombre_equipo', 'equipo_local', 'equipo_visitante'], axis=1) # Unión para incluir id_equipo_visitante
    partidos = pd.merge(partidos, jornadas[['id_jornada','numero_jornada','id_temporada','id_competicion']],
                        on = ['numero_jornada','id_temporada','id_competicion'],
                        how = 'left') \
        .drop(['numero_jornada','id_temporada','id_competicion'],axis=1) # Unión para incluir id_jornada

    return partidos

def crear_tabla_tiros(df_raw,jugadores,equipos,jugadores_equipos):
    """Crea una tabla de tiros."""
    
    df_raw = procesar_raw(df_raw)
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
    
    df_tiros = pd.merge(df_tiros_sin_id,
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
    
    df_tiros['id_jugador'] = df_tiros.apply(corregir_nombre_ambiguo, axis=1, args=(estructura,)) # Se asigna el id_jugador correspondiente a lanzamientos con ambigüedad.
    df_tiros['id_jugador'] = df_tiros['id_jugador'].astype('int')
    df_tiros = df_tiros \
        .drop(['nombre_jugador','id_local','id_visitante','id_temporada'],axis=1) \
        .reset_index(drop=True) \
        .rename({'resultado_local':'puntos_local',
                 'resultado_visitante':'puntos_visitante'},axis=1) 
    df_tiros = df_tiros[['id_jugador','x','y','descripcion','anotado','cuarto','tiempo','id_partido','puntos_local','puntos_visitante']] # Columnas de tiros reordenadas.

    return df_tiros