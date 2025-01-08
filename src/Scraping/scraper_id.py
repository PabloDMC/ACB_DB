import requests
from bs4 import BeautifulSoup
import re

def obtener_id_equipos(id_temporada):
    """
    Obtiene los IDs de los equipos que han jugado en Liga Endesa entre 2020 y 2024.

    Args:
        id_temporada (int): Año de inicio de la temporada.
        
    Returns:
        list: Una lista de ID, nombre y temporada de los equipos.
    """
    equipos = []
    id_regex = re.compile(r'/club/plantilla/id/(\d+)')
    url_jornada = f'https://www.acb.com/resultados-clasificacion/ver/temporada_id/{id_temporada}/competicion_id/1/jornada_numero/1'
    response = requests.get(url_jornada)
    soup = BeautifulSoup(response.content,features="html.parser")
        
    for equipo in soup.find_all('td', class_='nombre_equipo'):
        enlace = equipo.a['href']
        nombre_equipo = equipo.find('span', class_='nombre_largo').text
        id_club = id_regex.search(enlace).group(1)
        equipos.append({'id_club': id_club, 'nombre_equipo': nombre_equipo, 'id_temporada': id_temporada})
        
    return equipos

def obtener_id_jugadores(equipos):
    """
    Obtiene los IDs de los jugadores que han jugado en Liga Endesa entre 2020 y 2024.

    Args:
        equipos (list): lista de equipos 
    
    Returns:
        list: Una lista de ID, nombre, ID del equipo, nombre del equipo y temporada de los jugadores.
    """
    datos_jugadores = []
    for equipo in equipos:
        id_equipo = equipo['id_club']
        nombre_equipo = equipo['nombre_equipo']
        id_temporada = equipo['id_temporada']
        url_jornada = f'https://www.acb.com/club/plantilla/id/{id_equipo}/temporada_id/{id_temporada}'
        response = requests.get(url_jornada)
        soup = BeautifulSoup(response.content,features="html.parser")

        id_regex = re.compile(r'/jugador/ver/(\d+)-')
        for jugador in soup.find_all('div', class_='datos'):
            enlace = jugador.find('a')
            if enlace:
                id_jugador = id_regex.search(enlace['href'])
                nombre_jugador = enlace.get_text(strip=True)
                if id_jugador:
                    datos_jugadores.append({
                                'id_jugador': id_jugador.group(1),
                                'nombre_jugador': nombre_jugador,
                                'id_equipo': id_equipo,
                                'nombre_equipo': nombre_equipo,
                                'id_temporada': id_temporada,
                                'estado': 'activo'
                                })
            
        tabla_bajas = soup.find('table', class_='roboto defecto tabla_plantilla plantilla_bajas clasificacion tabla_ancho_completo')
        if tabla_bajas:
            for jugador in tabla_bajas.find_all('tr', class_='roboto_light'):
                enlace = jugador.find('a')
                if enlace:
                    id_jugador = id_regex.search(enlace['href'])
                    nombre_jugador = enlace.find('span', class_='nombre_corto').get_text(strip=True)
                    if id_jugador:
                        datos_jugadores.append({
                                    'id_jugador': id_jugador.group(1),
                                    'nombre_jugador': nombre_jugador,
                                    'id_equipo': id_equipo,
                                    'nombre_equipo': nombre_equipo,
                                    'id_temporada': id_temporada,
                                    'estado': 'baja'
                                })

    return datos_jugadores