from scraper_id import obtener_id_equipos,obtener_id_jugadores
import pandas as pd

lista_jugadores = []
for i in range(2020,2025):
    equipos = obtener_id_equipos(i)
    lista_jugadores.extend(obtener_id_jugadores(equipos))
    
df_jugadores = pd.DataFrame(lista_jugadores)
df_jugadores.to_csv('data/raw/df_jugadores.csv', index=False)