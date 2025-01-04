import subprocess
import sys
import os

def run_script(script_path, *args):
    """
    Ejecuta un script de Python como un subproceso.
    :param script_path: Ruta del script a ejecutar.
    :param args: Argumentos opcionales a pasar al script.
    """
    try:
        command = [sys.executable, script_path] + list(args)
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        print(f"Salida de {script_path}:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error al ejecutar {script_path}:\n{e.stderr}")
        sys.exit(1)

if __name__ == "__main__":
    # Define las rutas de los scripts a ejecutar
    base_dir = os.path.dirname(os.path.abspath(__file__))
    scripts = [
        {"path": os.path.join(base_dir, "src", "Scraping", "scrape_jornada.py"), "args": ["-j", "6", "-c", "liga_endesa", "-t", "2024"]},
        {"path": os.path.join(base_dir, "src", "Processing", "process_player_team.py"), "args": []},
        {"path": os.path.join(base_dir, "src", "Processing", "process_dynamic_data.py"), "args": ["-j", "6", "-c", "liga_endesa", "-t", "2024"]},
        {"path": os.path.join(base_dir, "src", "Database", "base_de_datos.py"), "args": []},
    ]

    # Ejecutar cada script en orden
    for script in scripts:
        print(f"Ejecutando: {script['path']} con argumentos: {script['args']}")
        run_script(script["path"], *script["args"])
