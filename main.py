import subprocess
import sys
import os
import argparse

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

def etapa_inicial(base_dir):
    """
    Realiza la etapa inicial de procesamiento:
    - Scrapea el histórico de datos.
    - Procesa los datos y crea y rellena las tablas en la base de datos.
    """
    print("=== Ejecutando la Etapa Inicial ===")
    scripts = [
        {"path": os.path.join(base_dir, "src", "Scraping", "scrape_shots.py"), "args": []},
        {"path": os.path.join(base_dir, "src", "Scraping", "scrape_players.py"), "args": []},
        {"path": os.path.join(base_dir, "src", "Processing", "process_static.py"), "args": []},
        {"path": os.path.join(base_dir, "src", "Processing", "process_data.py"), "args": ['inicial']},
        {"path": os.path.join(base_dir, "src", "Database", "base_de_datos.py"), "args": ['inicial']},
    ]

    for script in scripts:
        print(f"Ejecutando: {script['path']} con argumentos: {script['args']}")
        run_script(script["path"], *script["args"])
    print("=== Etapa Inicial Completada ===")

def etapa_actualizacion(base_dir, jornada, competicion, temporada):
    """
    Realiza la etapa de actualización:
    - Scrapea datos de una jornada específica.
    - Procesa los datos y actualiza las tablas dinámicas en la base de datos.
    """
    print(f"=== Ejecutando la Actualización para Jornada {jornada} ===")
    scripts = [
        {"path": os.path.join(base_dir, "src", "Scraping", "scrape_jornada.py"),
         "args": ["-j", str(jornada), "-c", competicion, "-t", str(temporada)]},
        {"path": os.path.join(base_dir, "src", "Processing", "process_data.py"),
         "args": ["actualizacion", "-j", str(jornada), "-c", competicion, "-t", str(temporada)]},
        {"path": os.path.join(base_dir, "src", "Database", "base_de_datos.py"),
         "args": ["actualizacion"]},
    ]

    for script in scripts:
        print(f"Ejecutando: {script['path']} con argumentos: {script['args']}")
        run_script(script["path"], *script["args"])
    print(f"=== Actualización de Jornada {jornada} Completada ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gestor de flujo de trabajo para el proyecto ACB_DB")
    parser.add_argument(
        "etapa",
        choices=["inicial", "actualizacion"],
        help="Etapa del flujo de trabajo: 'inicial' para el procesamiento histórico o 'actualizacion' para una jornada específica."
    )
    parser.add_argument("-j", "--jornada", type=int, help="Número de la jornada (para actualizaciones).")
    parser.add_argument("-c", "--competicion", type=str, help="Nombre de la competición (para actualizaciones).")
    parser.add_argument("-t", "--temporada", type=int, help="Año de la temporada (para actualizaciones).")

    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))

    if args.etapa == "inicial":
        etapa_inicial(base_dir)
    elif args.etapa == "actualizacion":
        if args.jornada is None or args.competicion is None or args.temporada is None:
            print("Error: Para 'actualizacion', debes proporcionar --jornada, --competicion y --temporada.")
            sys.exit(1)
        etapa_actualizacion(base_dir, args.jornada, args.competicion, args.temporada)
