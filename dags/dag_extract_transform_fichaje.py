from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os
import sys

# Añadir la carpeta include al PYTHONPATH
sys.path.append('/opt/airflow/include')

# Importar la función ETL modular
from etl.fichaje_etl import extract_and_transform_fichaje

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='dag_extract_transform_fichaje',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Extrae y transforma datos de fichaje desde MariaDB para predicción de RRHH',
) as dag:

    def run_etl():
        try:
            print("===== INICIO DAG: dag_extract_transform_fichaje =====")

            print("Obteniendo conexión ORIGEN: mariadb_mecagali")
            conn_origen = BaseHook.get_connection("mariadb_mecagali")
            conn_str_origen = f"mysql+pymysql://{conn_origen.login}:{conn_origen.password}@{conn_origen.host}:{conn_origen.port}/{conn_origen.schema}"

            print("Obteniendo conexión DESTINO: mariadb_mecagali_transf")
            conn_destino = BaseHook.get_connection("mariadb_mecagali_transf")
            conn_str_destino = f"mysql+pymysql://{conn_destino.login}:{conn_destino.password}@{conn_destino.host}:{conn_destino.port}/{conn_destino.schema}"

            print(f"Cadena conexión ORIGEN: {conn_str_origen}")
            print(f"Cadena conexión DESTINO: {conn_str_destino}")

            result = extract_and_transform_fichaje(conn_str_origen, conn_str_destino)

            print("ETL finalizado correctamente. Resultado:")
            print(result)
            print("===== FIN DAG =====")
        except Exception as e:
            print("ERROR durante la ejecución del DAG:")
            print(str(e))
            raise

    etl_task = PythonOperator(
        task_id='extract_transform_fichaje',
        python_callable=run_etl,
    )
