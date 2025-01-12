import argparse
from Utils.data_processing import actualizar_dataframe
from Utils.file_operations import cargar_datos_carpeta,ultimo_archivo
from process_dynamic import crear_tabla_jornadas, crear_tabla_partidos, crear_tabla_jugadores, crear_tabla_tiros
import pickle
import pandas as pd

def flujo_inicial():
    """Flujo para procesar datos iniciales y crear tablas."""
    
    # Cargar datos brutos
    with open("data/processed/static_data.pkl", "rb") as f:
        diccionario_static_df = pickle.load(f)
    equipos = diccionario_static_df['equipos']
    df_jugadores = pd.read_csv('data/raw/df_jugadores.csv')
    dict_copa = cargar_datos_carpeta('data/raw/copa_del_rey')
    dict_liga = cargar_datos_carpeta('data/raw/liga_endesa')
    dict_supercopa = cargar_datos_carpeta('data/raw/supercopa_endesa')
    df_copa = pd.concat(dict_copa.values(), ignore_index=True)
    df_liga = pd.concat(dict_liga.values(), ignore_index=True)
    df_supercopa = pd.concat(dict_supercopa.values(), ignore_index=True)
    df_raw = pd.concat([df_liga,df_copa,df_supercopa],ignore_index=True)

    # Procesar los datos brutos
    jugadores,jugadores_equipos = crear_tabla_jugadores(df_jugadores,equipos)
    jugadores_equipos = jugadores_equipos.reset_index().rename({'index':'id_jugador_equipo'},axis=1)
    jugadores_equipos.id_jugador_equipo += 1
    jornadas = crear_tabla_jornadas(df_raw)
    jornadas = jornadas.reset_index().rename({'index':'id_jornada'},axis=1)
    jornadas.id_jornada += 1
    partidos = crear_tabla_partidos(df_raw,jornadas,equipos)
    tiros = crear_tabla_tiros(df_raw,jugadores,equipos,jugadores_equipos)
    tiros = tiros.reset_index().rename({'index':'id_tiro'},axis=1)
    tiros.id_tiro +=1
    
    # Se guardan las tablas en un diccionario de dataframes en un archivo pickle
    players_dfs = {"jugadores": jugadores,"jugadores_equipos": jugadores_equipos}
    with open("data/processed/jugadores_equipos.pkl", "wb") as f:
        pickle.dump(players_dfs, f)
    print('Tablas de jugadores y jugadores_equipos creadas.')
    
    dynamic_dfs = {"jornadas": jornadas,"partidos": partidos,"tiros": tiros}
    with open("data/processed/dynamic_data.pkl", "wb") as f:
        pickle.dump(dynamic_dfs, f)
    print('Tablas de jornadas, partidos y tiros creadas.')


def flujo_actualizacion(jornada, competicion, temporada):
    """Flujo para actualizar datos dinámicos con nueva información."""
    
    # Cargar datos nuevos
    df_jugadores = pd.read_csv(ultimo_archivo('data/raw/new','df_jugadores'))
    with open("data/processed/dynamic_data.pkl", "rb") as f:
        diccionario_dynamic_df = pickle.load(f)
    with open("data/processed/static_data.pkl", "rb") as f:
        diccionario_static_df = pickle.load(f)
    with open("data/processed/jugadores_equipos.pkl", "rb") as f:
        diccionario_df = pickle.load(f)
    equipos = diccionario_static_df['equipos']
    jugadores_antiguo = diccionario_df['jugadores']
    jugadores_equipos_antiguo = diccionario_df['jugadores_equipos']
    jornadas_antiguo = diccionario_dynamic_df['jornadas']
    partidos_antiguo = diccionario_dynamic_df['partidos']
    tiros_antiguo = diccionario_dynamic_df['tiros']
    df_raw = pd.read_csv(f'data/raw/new/jornada_{jornada}_{competicion}_{temporada}.csv')
    
    # Procesar los nuevos datos y actualizar las tablas ya existentes
    jugadores_nuevo,jugadores_equipos_nuevo = crear_tabla_jugadores(df_jugadores,equipos)
    jugadores_equipos = actualizar_dataframe(jugadores_equipos_antiguo,jugadores_equipos_nuevo,'id_jugador_equipo')
    jugadores = pd.concat([jugadores_antiguo,jugadores_nuevo],ignore_index=True).drop_duplicates()
    jornadas_nuevo = crear_tabla_jornadas(df_raw)
    jornadas = actualizar_dataframe(jornadas_antiguo,jornadas_nuevo,'id_jornada')
    partidos_nuevo = crear_tabla_partidos(df_raw,jornadas,equipos)
    partidos = pd.concat([partidos_antiguo,partidos_nuevo],ignore_index=True).drop_duplicates()
    tiros_nuevo = crear_tabla_tiros(df_raw,jugadores,equipos,jugadores_equipos)
    tiros = actualizar_dataframe(tiros_antiguo,tiros_nuevo,'id_tiro')
    
    # Se guardan las tablas en un diccionario de dataframes en un archivo pickle
    diccionario_df = {"jugadores": jugadores,"jugadores_equipos": jugadores_equipos}
    with open("data/processed/jugadores_equipos.pkl", "wb") as f:
        pickle.dump(diccionario_df, f)
    print('Tablas de jugadores y jugadores_equipos actualizadas.')
    
    dataframes = {"jornadas": jornadas,"partidos": partidos,"tiros": tiros}
    with open("data/processed/dynamic_data.pkl", "wb") as f:
        pickle.dump(dataframes, f)
    print('Tablas de jornadas, partidos y tiros actualizadas.')


def main(etapa, jornada=None, competicion=None, temporada=None):
    if etapa == "inicial":
        flujo_inicial()
    elif etapa == "actualizacion":
        flujo_actualizacion(jornada, competicion, temporada)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Procesar datos de la ACB")
    parser.add_argument("etapa", choices=["inicial", "actualizacion"], help="Etapa de ejecución")
    parser.add_argument("-j", "--jornada", type=int, help="Número de la jornada (para actualizaciones)")
    parser.add_argument("-c", "--competicion", type=str, help="Competición (para actualizaciones)")
    parser.add_argument("-t", "--temporada", type=int, help="Temporada (para actualizaciones)")
    args = parser.parse_args()

    main(args.etapa, args.jornada, args.competicion, args.temporada)
