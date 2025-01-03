import argparse
from Clases.scraper_id import obtener_id_equipos,obtener_id_jugadores
from Clases.scraper import ScraperACB
from Utils.utils import guardar_datos_csv
from datetime import datetime
import pandas as pd

def scrape_jornada(jornada,competicion):
    try:
        dict_competiciones = {'liga_endesa':1,'copa_del_rey':2,'supercopa_endesa':3}
        scraper = ScraperACB()
        nuevos_datos = scraper.obtener_tiros_jornada(jornada, id_temporada=2024, id_competicion=dict_competiciones[competicion])

        file_path = 'data/raw/new/'
        guardar_datos_csv(nuevos_datos, f"{file_path}jornada_{jornada}_{competicion}.csv")
        print(f"Datos de la jornada {jornada} guardados en {file_path}.")
    except Exception as e:
        print(f"Error durante el scraping de la jornada {jornada}: {e}")
        raise

def scrape_jugadores():
    try:
        fecha = datetime.now()
        fecha_sin_hora = fecha.strftime('%d-%m-%Y')
        tipos = {'id_jugador':'int','nombre_jugador':'string','id_equipo':'int','nombre_equipo':'string','id_temporada':'int','estado':'string'}
        list_equipos = obtener_id_equipos(2024)
        list_jugadores = obtener_id_jugadores(list_equipos)
        
        df_jugadores = pd.DataFrame(list_jugadores)
        df_jugadores = df_jugadores.astype(tipos)

        file_path = 'data/raw/new/'
        df_jugadores.to_csv(f'{file_path}df_jugadores_{fecha_sin_hora}.csv',index=False)
        print(f"Datos de jugadores guardados en {file_path}.")
    except Exception as e:
        print(f"Error durante el scraping de jugadores y equipos: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraping de datos de una jornada específica.")
    parser.add_argument("-j", "--jornada", type=int, required=True, help="Número de la jornada.")
    parser.add_argument("-c", "--competicion", type=str, required=True, help="ID de la competicion.")
    args = parser.parse_args()

    scrape_jornada(args.jornada, args.competicion)
    scrape_jugadores()