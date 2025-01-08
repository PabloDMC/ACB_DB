import os
import argparse
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
import numpy as np
import pickle
from dotenv import load_dotenv

def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)

def adapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)

def adapt_numpy_bool(numpy_bool):
    return AsIs(bool(numpy_bool))

register_adapter(np.int64, adapt_numpy_int64)
register_adapter(np.float64, adapt_numpy_float64)
register_adapter(np.bool_, adapt_numpy_bool)

class BaseDeDatos:
    def __init__(self):
        """Inicializa la clase y carga las configuraciones."""
        load_dotenv()
        self.connection = None
        self.dbname = os.getenv("DB_NAME", "postgres")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")

    def _connect(self, dbname=None, autocommit=False):
        """Crea una conexión a la base de datos especificada."""
        dbname = dbname or self.dbname
        try:
            connection = psycopg2.connect(
                dbname=dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            connection.autocommit = autocommit  # Establece el modo de autocommit
            return connection
        except Exception as e:
            raise RuntimeError(f"Error al conectar a la base de datos '{dbname}': {e}")

    def ejecutar_script_sql(self, script_path):
        """Ejecuta un script SQL desde un archivo."""
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"El archivo {script_path} no existe.")
        with self.connection.cursor() as cursor:
            with open(script_path, 'r') as file:
                cursor.execute(file.read())
            self.connection.commit()

    def create_database_if_not_exists(self, script_path):
        """Crea la base de datos si no existe y configura las tablas."""
        try:
            # Conectar a la base de datos 'postgres' para crear la base de datos
            print("Conectando a 'postgres' para crear la base de datos si no existe...")
            temp_connection = psycopg2.connect(
                dbname="postgres",
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            temp_connection.autocommit = True  # Importante: autocommit debe estar activado

            with temp_connection.cursor() as cursor:
                # Verificar si la base de datos ya existe
                cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", [self.dbname])
                if not cursor.fetchone():
                    cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.dbname)))
                    print(f"Base de datos '{self.dbname}' creada.")
                else:
                    print(f"La base de datos '{self.dbname}' ya existe.")

            # Cerrar la conexión temporal
            temp_connection.close()

            # Ahora conectar a la base de datos recién creada
            self.connection = self._connect(self.dbname)

            # Ejecutar el script para configurar las tablas
            self.ejecutar_script_sql(script_path)
            print("Base de datos configurada según el esquema proporcionado.")

        except Exception as e:
            print(f"Error al crear o configurar la base de datos: {e}")
            if self.connection:
                self.connection.rollback()

    def update_table(self, table_name, new_data):
        """Actualiza la tabla de la base de datos con nuevas filas."""
        try:
            if self.connection is None:
                raise RuntimeError("No hay conexión activa a la base de datos.")
            
            primary_key_column = new_data.columns[0]  # Suponemos que la primera columna es la clave primaria

            # Obtener las filas existentes en la tabla
            with self.connection.cursor() as cursor:
                cursor.execute(sql.SQL("SELECT {} FROM {}").format(
                    sql.Identifier(primary_key_column),
                    sql.Identifier(table_name)
                ))
                existing_rows = {row[0] for row in cursor.fetchall()}  # Conjunto de claves primarias existentes

            # Filtrar las filas nuevas que no están en la tabla
            new_rows = new_data[~new_data[primary_key_column].isin(existing_rows)]

            # Si hay filas nuevas, insertarlas en la tabla
            if not new_rows.empty:
                insert_columns = list(new_rows.columns)  # Tomamos todas las columnas del DataFrame
                insert_values = [tuple(new_rows.iloc[i]) for i in range(new_rows.shape[0])]

                # Formamos la consulta SQL de inserción
                insert_query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(map(sql.Identifier, insert_columns))
                )

                with self.connection.cursor() as cursor:
                    # Usamos execute_values para insertar múltiples filas
                    execute_values(cursor, insert_query, insert_values)
                    self.connection.commit()  # Hacemos commit de la transacción

                    print(f"{len(new_rows)} nuevas filas insertadas en la tabla '{table_name}'.")
            else:
                print(f"No hay nuevas filas para insertar en la tabla '{table_name}'.")

        except Exception as e:
            print(f"Error al actualizar la tabla '{table_name}': {e}")
            if self.connection:
                self.connection.rollback()

    def close_connection(self):
        """Cierra la conexión a la base de datos."""
        if self.connection:
            self.connection.close()
            print("Conexión cerrada.")

def main(etapa):
    """
    Ejecuta el flujo de trabajo según la etapa especificada.
    Args:
        etapa (str): 'inicial' para crear la base de datos desde cero o 'actualizacion' para actualizar tablas existentes.
    """
    acb_db = BaseDeDatos()  # Crear una instancia de la conexión a la base de datos

    try:
        if etapa == "inicial":
            print("=== Etapa Inicial: Configurando base de datos desde cero ===")
            
            # Crear la base de datos y el esquema inicial
            acb_db.create_database_if_not_exists("src/database/schema.sql")
            
            # Cargar y llenar tablas estáticaa
            with open("data/processed/static_data.pkl", "rb") as f:
                diccionario_static = pickle.load(f)
            acb_db.update_table('clubes', diccionario_static['clubes'])
            acb_db.update_table('competiciones', diccionario_static['competiciones'])
            acb_db.update_table('temporadas', diccionario_static['temporadas'])
            acb_db.update_table('equipos', diccionario_static['equipos'])

            # Cargar y llenar tablas dinámicas
            print("Procesando y rellenando tablas dinámicas...")
            with open("data/processed/jugadores_equipos.pkl", "rb") as f:
                diccionario_jugadores_equipos = pickle.load(f)
            with open("data/processed/dynamic_data.pkl", "rb") as f:
                diccionario_dynamic = pickle.load(f)
            
            acb_db.update_table('jugadores', diccionario_jugadores_equipos['jugadores'])
            acb_db.update_table('jugadores_equipos', diccionario_jugadores_equipos['jugadores_equipos'])
            acb_db.update_table('jornadas', diccionario_dynamic['jornadas'])
            acb_db.update_table('partidos', diccionario_dynamic['partidos'])
            acb_db.update_table('tiros', diccionario_dynamic['tiros'])
            
            # Ejecutar script SQL para el esquema presentation
            acb_db.ejecutar_script_sql('src/Database/presentation.sql')
            print("=== Inicialización completada ===")
        
        elif etapa == "actualizacion":
            print("=== Etapa Actualización: Actualizando tablas dinámicas ===")
            
            # Cargar datos dinámicos
            with open("data/processed/jugadores_equipos.pkl", "rb") as f:
                diccionario_jugadores_equipos = pickle.load(f)
            with open("data/processed/dynamic_data.pkl", "rb") as f:
                diccionario_dynamic = pickle.load(f)
            
            # Actualizar tablas dinámicas
            acb_db.update_table('jugadores', diccionario_jugadores_equipos['jugadores'])
            acb_db.update_table('jugadores_equipos', diccionario_jugadores_equipos['jugadores_equipos'])
            acb_db.update_table('jornadas', diccionario_dynamic['jornadas'])
            acb_db.update_table('partidos', diccionario_dynamic['partidos'])
            acb_db.update_table('tiros', diccionario_dynamic['tiros'])
            
            # Ejecutar script SQL para el esquema presentation
            acb_db.ejecutar_script_sql('src/Database/presentation.sql')
            print("=== Actualización completada ===")
        
        else:
            print(f"Etapa '{etapa}' no reconocida. Usa 'inicial' o 'actualizacion'.")
    
    except Exception as e:
        print(f"Se produjo un error: {e}")
    finally:
        acb_db.close_connection()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Procesar base de datos de la ACB")
    parser.add_argument(
        "etapa",
        choices=["inicial", "actualizacion"],
        help="Etapa de operación: 'inicial' para configurar la base de datos o 'actualizacion' para actualizarla."
    )
    args = parser.parse_args()
    main(args.etapa)
