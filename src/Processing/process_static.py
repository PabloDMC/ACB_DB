import pickle
import pandas as pd
from Scraping.scraper_id import obtener_id_equipos

def main():
    """
    Creación de las tablas 'equipos','clubes','temporadas' y 'competiciones' de la base de datos. 
    Estas tablas son estáticas, no se actualizarán con el paso del tiempo.
    """
    
    # Creación de la tabla equipos.
    list_equipos = []
    for temporada in range(2020,2025):
        list_equipos.extend(obtener_id_equipos(temporada))
    df_equipos = pd.DataFrame(list_equipos)
    df_equipos = df_equipos.astype({'id_club': 'int'})
    df_equipos = pd.concat([df_equipos,
                        pd.DataFrame({'id_club':[28,5,15],
                            'nombre_equipo':['Lenovo Tenerife','Dreamland Gran Canaria','Coosur Real Betis'],
                            'id_temporada':[2020,2022,2022]})
                        ])
    df_equipos.replace({'nombre_equipo':{'Valencia Basket Club':'Valencia Basket'}},inplace=True)
    equipos = df_equipos \
        .replace({'id_temporada':{2020:1,
                                2021:2,
                                2022:3,
                                2023:4,
                                2024:5}}) \
        .sort_values(by=['id_club','id_temporada']) \
        .reset_index(drop=True) \
        .reset_index() \
        .rename(columns={'index':'id_equipo'})                        
    equipos.id_equipo += 1
    
    # Creación de la tabla temporadas.
    temporadas = pd.DataFrame([[1,2020,2021,'2020-2021'],
                                [2,2021,2022,'2021-2022'],
                                [3,2022,2023,'2022-2023'],
                                [4,2023,2024,'2023-2024'],
                                [5,2024,2025,'2024-2025']],
                                columns=['id_temporada','anio_inicio','anio_fin','temporada'])

    # Creación de la tabla clubes.
    nombre_clubes={'Acunsa GBC': 'Gipuzkoa Basket Club',
                    'Barça': 'Fútbol Club Barcelona',
                    'BAXI Manresa': 'Bàsquet Manresa',
                    'Casademont Zaragoza': 'Basket Zaragoza 2002, S.A.D.',
                    'Coosur Real Betis': 'Real Betis Baloncesto',
                    'Coviran Granada': 'Fundación Club Baloncesto Granada',
                    'Herbalife Gran Canaria': 'Club Baloncesto Gran Canaria',
                    'Hereda San Pablo Burgos': 'Club Baloncesto San Pablo Burgos',
                    'Hiopos Lleida': 'Força Lleida Club Esportiu',
                    'Iberostar Tenerife': 'Club Baloncesto Canarias',
                    'Leyma Coruña': 'Club Básquet Coruña',
                    'Monbus Obradoiro': 'Obradoiro Clube de Amigos do Baloncesto, S.A.D',
                    'MoraBanc Andorra': 'Bàsquet Club Andorra',
                    'Movistar Estudiantes': 'Club Estudiantes de Baloncesto',
                    'Real Madrid': 'Real Madrid Baloncesto',
                    'RETAbet Bilbao Basket': 'Club Basket Bilbao Berri, S.A.D.',
                    'Río Breogán': 'Club Baloncesto Breogán',
                    'TD Systems Baskonia': 'Club Deportivo Baskonia Vitoria Gasteiz',
                    'UCAM Murcia CB': 'UCAM Murcia Club de Baloncesto',
                    'Unicaja' : 'Club Baloncesto Málaga S.A.D.',
                    'Urbas Fuenlabrada': 'Club Baloncesto Fuenlabrada, S.A.D.',
                    'Valencia Basket Club': 'Valencia Basket Club, S.A.D.',
                    'Zunder Palencia': 'Palencia Baloncesto'}
    clubes = df_equipos[['id_club','nombre_equipo']] \
                .drop_duplicates(['id_club']) \
                .rename(columns={'nombre_equipo':'nombre_oficial'}) \
                .replace({'nombre_oficial':nombre_clubes}) \
                .sort_values(by='id_club')

    # Creación de la tabla competiciones.
    competiciones = pd.DataFrame([[1,'Liga Endesa'],[2,'Copa del Rey'],[3,'Supercopa Endesa']],
                                columns=['id_competicion','nombre_competicion'])

    # Se guardan las tablas en un diccionario de dataframes en un archivo pickle.
    diccionario_df = {"equipos": equipos,
                      "clubes": clubes,
                      "competiciones": competiciones,
                      "temporadas": temporadas}
    with open("data/processed/static_data.pkl", "wb") as f:
        pickle.dump(diccionario_df, f)

if __name__ == "__main__":
    main()