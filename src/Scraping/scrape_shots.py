from scraper import ScraperACB
from Utils.file_operations import guardar_datos_csv

scraper = ScraperACB()
raw_path = '../data/raw/'

dict_competiciones = {'liga_endesa':1,'copa_del_rey':2,'supercopa_endesa':3}
id_temporadas = [2020,2021,2022,2023,2024]

for competicion,id_competicion in dict_competiciones.items():
    for anio in id_temporadas:
        tiros = scraper.obtener_tiros_competicion(id_competicion,anio)
        nombre_archivo = f'{competicion}_{anio}'
        guardar_datos_csv(tiros, f'{raw_path}{nombre_archivo}.csv')
        print(f'Datos guardados para {nombre_archivo}')