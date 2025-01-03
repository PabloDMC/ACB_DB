import pandas as pd
import os
import glob
from datetime import datetime

def convertir_df(data):
    """
    Convierte una lista de listas en un dataframe.

    Args:
        data (list[list]): Datos a guardar.
    """
    df = pd.DataFrame(data, columns=[
                    'nombre', 'x', 'y', 'cuarto', 'tiempo', 'equipo_local',
                    'resultado_local', 'resultado_visitante', 'equipo_visitante', 
                    'descripcion', 'anotado', 'id_partido',
                    'jornada', 'temporada', 'competicion', 'playoff'
                ])
    df['x'] = df['x'].astype(float).round(6)
    df['y'] = df['y'].astype(float).round(6)
    df = df.astype({'resultado_local':'int',
                    'resultado_visitante':'int',
                    'anotado':'bool',
                    'id_partido':'int',
                    'jornada':'int',
                    'playoff':'bool'})
    return df

def guardar_datos_csv(data, archivo):
    """
    Guarda los datos en un archivo CSV.

    Args:
        data (list): Datos a guardar.
        archivo (str): Nombre del archivo de salida.
    """
    df = convertir_df(data)
    df.to_csv(archivo, index=False)
    print(f"Datos guardados en {archivo}")
    
def cargar_datos_carpeta(directorio):
    """
    Lee todos los archivos CSV en el directorio especificado y los carga en un diccionario.

    Args:
        directorio (str): Ruta a la carpeta que contiene los archivos CSV.

    Returns:
        dict: Un diccionario donde las claves son los nombres de los archivos (sin extensión) 
              y los valores son DataFrames con los datos de esos archivos.
    """
    dataframes = {}
    for archivo in os.listdir(directorio):
        if archivo.endswith('.csv'):  # Solo procesar archivos CSV
            nombre = os.path.splitext(archivo)[0]  # Nombre del archivo sin la extensión
            ruta_completa = os.path.join(directorio, archivo)
            try:
                dataframes[nombre] = pd.read_csv(ruta_completa)
                print(f"Archivo {archivo} cargado correctamente.")
            except Exception as e:
                print(f"Error al cargar {archivo}: {e}")
    return dataframes

def combinar_datos(dataframes):
    """
    Combina todos los DataFrames de un diccionario en un único DataFrame.

    Args:
        dataframes (dict): Diccionario de DataFrames a combinar.

    Returns:
        pd.DataFrame: Un único DataFrame combinado con una columna que identifica el origen.
    """
    for nombre, df in dataframes.items():
        df['origen'] = nombre  # Agregar una columna para identificar el archivo origen
    datos_combinados = pd.concat(dataframes.values(), ignore_index=True)
    return datos_combinados

def extraer_fecha(nombre_archivo):
    """
    Función para extraer la fecha del nombre de un archivo csv.
    
    Args:
        nombre_archivo (str): El nombre del archivo en formato 'jugadores_DD-MM-YYYY.csv',
                            donde 'DD-MM-YYYY' representa la fecha.

    Returns:
        datetime.datetime: Un objeto datetime que representa la fecha extraída del nombre del archivo.
    """
    fecha_str = nombre_archivo.split('_')[-1].replace('.csv', '')
    return datetime.strptime(fecha_str, '%d-%m-%Y')

def ultimo_archivo(directorio,nombre_archivo):
    """
    Busca y retorna el archivo más reciente en un directorio que coincide con el patrón de nombre especificado,
    basado en la fecha contenida en el nombre del archivo. Los archivos deben tener el formato 'nombre_archivo_DD-MM-YYYY.csv'.
    
    Args:
        directorio (str): La ruta del directorio donde se encuentran los archivos.
        nombre_archivo (str): El prefijo del nombre de los archivos que se buscan, el cual debe seguir el formato 'nombre_archivo_DD-MM-YYYY.csv'.

    Returns:
        str: La ruta completa del archivo más reciente encontrado que sigue el patrón, con la fecha más reciente en el nombre del archivo.
            Si no se encuentra ningún archivo que coincida, la función devuelve una lista vacía.
    """
    archivos = glob.glob(os.path.join(directorio, f'{nombre_archivo}_*.csv'))
    archivos_ordenados = sorted(archivos, key=extraer_fecha, reverse=True)
    return archivos_ordenados[0] if archivos_ordenados else None