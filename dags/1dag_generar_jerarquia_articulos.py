from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import pandas as pd
import sqlalchemy
import traceback

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='1dag_generar_jerarquia_articulos',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Genera jerarquía de artículos considerando solo dos niveles: Padre e Hijo',
) as dag:

    def generar_jerarquia_articulos():
        try:
            print("Conectando a bases de datos...")
            conn_origen = BaseHook.get_connection("mariadb_mecagali")
            engine_origen = sqlalchemy.create_engine(
                f"mysql+pymysql://{conn_origen.login}:{conn_origen.password}@{conn_origen.host}:{conn_origen.port}/{conn_origen.schema}"
            )

            conn_destino = BaseHook.get_connection("mariadb_mecagali_transf")
            engine_destino = sqlalchemy.create_engine(
                f"mysql+pymysql://{conn_destino.login}:{conn_destino.password}@{conn_destino.host}:{conn_destino.port}/{conn_destino.schema}"
            )
            print("Conexiones establecidas")

            # Crear la tabla si no existe
            print("Creando tabla 'jerarquia_articulos' si no existe...")
            create_table_query = """
                CREATE TABLE IF NOT EXISTS jerarquia_articulos (
                    ID_ARTICULO INT,
                    ID_VERSION_ARTICULO INT,
                    ID_COMPOSICION INT,
                    DESC_ARTICULO VARCHAR(255),
                    ID_VERSION INT,
                    VER_ARTICULO_PADRE INT,
                    VER_ARTICULO_HIJO INT,
                    CANTIDAD DECIMAL(10,2),
                    CODIGO_PADRE VARCHAR(100),
                    CODIGO_ARTICULO VARCHAR(100),
                    Nivel INT,
                    Tipo VARCHAR(50)
                );
            """
            engine_destino.execute(create_table_query)

            # Limpiar la tabla antes de insertar nuevos datos
            print("Limpiando la tabla 'jerarquia_articulos' antes de insertar nuevos datos...")
            engine_destino.execute("TRUNCATE TABLE jerarquia_articulos;")

            # Consulta para el nivel 1 (Padre-Hijo directo)
            query_nivel_1 = """
                INSERT INTO jerarquia_articulos
                SELECT 
                    c.ID_ARTICULO,
                    c.ID_VERSION_ARTICULO,
                    c.ID_COMPOSICION,
                    c.DESC_ARTICULO,
                    c.ID_VERSION,
                    c.VER_ARTICULO_PADRE,
                    c.VER_ARTICULO_HIJO,
                    c.CANTIDAD, 
                    p.CODIGO_INTERNO AS CODIGO_PADRE,
                    h.CODIGO_INTERNO AS CODIGO_ARTICULO,
                    1 AS Nivel,
                    'Hijo' AS Tipo
                FROM mecagali.PROD_VISTA_COMPOSICIONES c
                JOIN mecagali.PROD_VISTA_ARTICULOS p 
                    ON c.VER_ARTICULO_PADRE = p.ID_VERSION_ARTICULO
                    AND p.by_default = 1
                JOIN mecagali.PROD_VISTA_ARTICULOS h 
                    ON c.VER_ARTICULO_HIJO = h.ID_VERSION_ARTICULO
                    AND h.by_default = 1;
            """
            print("Ejecutando consulta para el nivel 1...")
            engine_destino.execute(query_nivel_1)
            print("Nivel 1 (Padre-Hijo directo) completado")

            # Consulta para el nivel 2 (Hijo que también es Padre)
            query_nivel_2 = """
                INSERT INTO jerarquia_articulos
                SELECT 
                    sub_c.ID_ARTICULO,
                    sub_c.ID_VERSION_ARTICULO,
                    sub_c.ID_COMPOSICION,
                    sub_c.DESC_ARTICULO,
                    sub_c.ID_VERSION,
                    sub_c.VER_ARTICULO_PADRE,
                    sub_c.VER_ARTICULO_HIJO,
                    sub_c.CANTIDAD,
                    r.CODIGO_ARTICULO AS CODIGO_PADRE,
                    h.CODIGO_INTERNO AS CODIGO_ARTICULO,
                    2 AS Nivel,
                    'Hijo' AS Tipo
                FROM mecagali.PROD_VISTA_COMPOSICIONES sub_c
                JOIN jerarquia_articulos r 
                    ON sub_c.VER_ARTICULO_PADRE = r.VER_ARTICULO_HIJO
                JOIN mecagali.PROD_VISTA_ARTICULOS h
                    ON sub_c.VER_ARTICULO_HIJO = h.ID_VERSION_ARTICULO
                    AND h.by_default = 1;
            """
            print("Ejecutando consulta para el nivel 2...")
            engine_destino.execute(query_nivel_2)
            print("Nivel 2 (Hijo-Padre) completado")

            print("Jerarquía generada correctamente en 'jerarquia_articulos'")

        except Exception as e:
            print("Error durante la ejecución del DAG:")
            print(traceback.format_exc())
            raise e

    # Definir la tarea del DAG
    tarea_generar_jerarquia = PythonOperator(
        task_id='generar_jerarquia_articulos',
        python_callable=generar_jerarquia_articulos
    )
