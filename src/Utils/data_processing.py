import regex as re
import unicodedata
from itertools import combinations
import pandas as pd

def quitar_acento(texto):
    return ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )

def transformar_nombre(nombre):
    partes = re.findall(r"\p{L}+['-]?\p{L}*\.?|\p{L}+[\.'-]?\p{L}*", nombre)

    if len(partes) < 2:
        return nombre

    primer_nombre = partes[0]
    if primer_nombre.isupper() and len(primer_nombre) > 1:
        return nombre

    inicial = quitar_acento(primer_nombre[0].upper()) + "."
    apellido = " ".join(partes[1:])
            
    return f"{inicial} {apellido}"

def corregir_nombre_ambiguo(row,estructura):
    clave = (row['nombre_jugador'], row['id_temporada'])
    if clave in estructura:
        for (id_jugador1, id_equipo1), (id_jugador2, id_equipo2) in combinations(estructura[clave], 2):
            if ((row['id_local'] == id_equipo1) & (row['id_visitante'] != id_equipo2)) | \
               ((row['id_local'] != id_equipo2) & (row['id_visitante'] == id_equipo1)) | \
               ((row['id_local'] == id_equipo1) & (row['id_visitante'] == id_equipo2) & (row['x']<=230)) | \
               ((row['id_local'] == id_equipo2) & (row['id_visitante'] == id_equipo1) & (row['x']>230)):
                    return id_jugador1
            elif ((row['id_local'] == id_equipo2) & (row['id_visitante'] != id_equipo1)) | \
                ((row['id_local'] != id_equipo1) & (row['id_visitante'] == id_equipo2)) | \
                ((row['id_local'] == id_equipo2) & (row['id_visitante'] == id_equipo1) & (row['x']<=230)) | \
                ((row['id_local'] == id_equipo1) & (row['id_visitante'] == id_equipo2) & (row['x']>230)):
                    return id_jugador2
    elif row['nombre_jugador'] == 'R. Macoha':
        return 20213018
    else:
        return row['id_jugador']
    
def actualizar_dataframe(df1, df2, columna):
    """
    Combina dos DataFrames, actualizando una columna específica con valores autoincrementales
    para las nuevas filas que aparecen en `df2` pero no en `df1`.

    Parámetros:
    - df1 (pd.DataFrame): DataFrame original que contiene la columna a actualizar.
    - df2 (pd.DataFrame): DataFrame con nuevos datos que se quieren combinar con `df1`.
    - columna (str): Nombre de la columna que se actualizará con valores autoincrementales para las filas nuevas.

    Retorna:
    - pd.DataFrame: Un nuevo DataFrame que contiene todas las filas combinadas con la columna actualizada.
    """
    if columna in df2.columns:
        df2 = df2.drop(columns=[columna])
    merge_columns = [col for col in df1.columns if col != columna]
    new_rows = df2[~df2.apply(tuple, axis=1).isin(df1[merge_columns].apply(tuple, axis=1))]
    max_id = df1[columna].max()
    new_rows.loc[:, columna] = range(max_id + 1, max_id + 1 + len(new_rows))
    combined_df = pd.concat([df1, new_rows]).reset_index(drop=True)

    return combined_df
