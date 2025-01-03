import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs
import pandas as pd
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
        """Inicializa la conexión a la base de datos PostgreSQL."""
        load_dotenv()
        self.connection = None
        self.dbname=os.getenv("DB_NAME", "postgres")
        self.user=os.getenv("DB_USER")
        self.password=os.getenv("DB_PASSWORD")
        self.host=os.getenv("DB_HOST")
        self.port=os.getenv("DB_PORT")
        self._connect_to_database()

    def _connect_to_database(self):
        """Establece conexión a PostgreSQL usando las variables de entorno.
        
        Si 'DB_NAME' no está definida, conecta a la base de datos 'postgres' por defecto.
        """
        try:
            print(f"Conectando a la base de datos '{self.dbname}'...")
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            print(f"Conexión a la base de datos '{self.dbname}' establecida correctamente.")
        except Exception as e:
            print(f"Error al conectar a la base de datos: {e}")
            raise
        
    def ejecutar_script_sql(self, script_path):
        """Ejecuta un script SQL desde un archivo."""
        if not os.path.exists(script_path):
            raise FileNotFoundError(f"El archivo {script_path} no existe.")
        with self.connection.cursor() as cursor:
            with open(script_path, 'r') as file:
                cursor.execute(file.read())
            self.connection.commit()  

    def create_database_if_not_exists(self, script_path):
        """Crea la base de datos si no existe y configura las tablas utilizando el script script_path."""
        try:
            # Establecer conexión a la base de datos postgres (por defecto)
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", [self.dbname])
                if not cursor.fetchone():
                    cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.dbname)))
                    print(f"Base de datos '{self.dbname}' creada.")
                else:
                    print(f"La base de datos '{self.dbname}' ya existe.")

            '''# Reconectar a la base de datos recién creada (o existente)
            self.connection.close()  # Cerrar la conexión actual (a postgres)
            self.connection = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name
            )
            self.connection.autocommit = False  # Rehabilitar transacciones explícitas
            '''
            # Ejecutar el script schema.sql para crear las tablas
            self.ejecutar_script_sql(script_path)
            print("Base de datos configurada según el esquema proporcionado.")
        except Exception as e:
            print(f"Error al crear o configurar la base de datos: {e}")
            if self.connection:
                self.connection.rollback() 

    def update_table(self, table_name, new_data):
        """Actualiza la tabla de la base de datos con nuevas filas.
        
        Args:
            table_name (str): Nombre de la tabla a actualizar.
            new_data (pd.DataFrame): Datos nuevos que se insertarán en la tabla.
        """
        try:
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
            self.connection.rollback()  # Hacemos rollback en caso de error     
    
    def close_connection(self):
        """Cierra la conexión a la base de datos."""
        if self.connection:
            self.connection.close()
            print("Conexión cerrada.")

if __name__ == "__main__":
    db_handler = BaseDeDatos()
    '''
    schema_path = "src/database/schema.sql"
    db_handler.create_database_if_not_exists(schema_path)
    '''
    with open("data/processed/jugadores_equipos.pkl", "rb") as f:
        diccionario_jugadores_equipos = pickle.load(f)
    '''with open("data/processed/static_data.pkl", "rb") as f:
        diccionario_static = pickle.load(f)'''
    with open("data/processed/dynamic_data.pkl", "rb") as f:
        diccionario_dynamic = pickle.load(f)
    jugadores = diccionario_jugadores_equipos['jugadores']
    jugadores_equipos = diccionario_jugadores_equipos['jugadores_equipos']
    '''equipos = diccionario_static['equipos']
    clubes = diccionario_static['clubes']
    competiciones = diccionario_static['competiciones']
    temporadas = diccionario_static['temporadas']'''
    jornadas = diccionario_dynamic['jornadas']
    partidos = diccionario_dynamic['partidos']
    tiros = diccionario_dynamic['tiros']
    
    db_handler.update_table('jugadores', jugadores)
    '''db_handler.update_table('clubes', clubes)
    db_handler.update_table('competiciones', competiciones)
    db_handler.update_table('temporadas', temporadas)
    db_handler.update_table('equipos', equipos)'''
    db_handler.update_table('jornadas', jornadas)
    db_handler.update_table('partidos', partidos)
    db_handler.update_table('tiros', tiros)
    db_handler.update_table('jugadores_equipos', jugadores_equipos)
    
    db_handler.close_connection()
