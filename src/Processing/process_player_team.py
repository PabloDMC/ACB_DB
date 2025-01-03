import pandas as pd
import pickle
from Utils.utils import ultimo_archivo
from Utils.data_processing import transformar_nombre,actualizar_dataframe

def main():
    df_jugadores_nuevo = pd.read_csv(ultimo_archivo('data/raw/new','df_jugadores'))
    with open("data/processed/static_data.pkl", "rb") as f:
        diccionario_static_df = pickle.load(f)
    with open("data/processed/jugadores_equipos.pkl", "rb") as f:
        diccionario_df = pickle.load(f)
    equipos = diccionario_static_df['equipos']
    df_jugadores = diccionario_df['jugadores']
    df_jugadores_equipos = diccionario_df['jugadores_equipos']
    
    df_jugadores_nuevo.replace({'nombre_equipo':{'Valencia Basket Club':'Valencia Basket'}},inplace=True)
    df_jugadores_nuevo["nombre_jugador"] = df_jugadores_nuevo['nombre_jugador'].apply(transformar_nombre)
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
    df_jugadores_nuevo.replace({'nombre_jugador':jugadores_distintos},inplace=True)

    df_jugadores_equipos_nuevo = df_jugadores_nuevo \
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
    df_jugadores_equipos_nuevo = df_jugadores_equipos_nuevo[['id_jugador','id_equipo','id_temporada']]
    
    jugadores_equipos = actualizar_dataframe(df_jugadores_equipos,df_jugadores_equipos_nuevo,'id_jugador_equipo')
    
    df_jugadores_nuevo = df_jugadores_nuevo.loc[:,['id_jugador','nombre_jugador']].sort_values(by='id_jugador').drop_duplicates()
    jugadores = pd.concat([df_jugadores,df_jugadores_nuevo],ignore_index=True).drop_duplicates()
    
    diccionario_df = {"jugadores": jugadores,
                      "jugadores_equipos": jugadores_equipos}
    with open("data/processed/jugadores_equipos.pkl", "wb") as f:
        pickle.dump(diccionario_df, f)
    print('Tablas de jugadores y jugadores_equipos actualizadas.')

if __name__ == "__main__":
    main()