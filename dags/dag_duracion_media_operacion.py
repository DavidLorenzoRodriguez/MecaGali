from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import pandas as pd
import sqlalchemy
import os
import sys

# Aseguramos que se pueda importar lógica de ETL si la necesitamos en módulos separados
sys.path.append('/opt/airflow/include')


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='dag_duracion_media_operacion',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Calcula la duración media por operación desde vmrf_signing_control_workers',
) as dag:

    def calcular_duracion_media_operacion():
        print("===== INICIO DAG: dag_duracion_media_operacion =====")

        # Conexion origen
        print("Obteniendo conexión ORIGEN: mariadb_mecagali")
        conn_origen = BaseHook.get_connection("mariadb_mecagali")
        conn_str_origen = f"mysql+pymysql://{conn_origen.login}:{conn_origen.password}@{conn_origen.host}:{conn_origen.port}/{conn_origen.schema}"
        engine_origen = sqlalchemy.create_engine(conn_str_origen)

        # Conexion destino
        print("Obteniendo conexión DESTINO: mariadb_mecagali_transf")
        conn_destino = BaseHook.get_connection("mariadb_mecagali_transf")
        conn_str_destino = f"mysql+pymysql://{conn_destino.login}:{conn_destino.password}@{conn_destino.host}:{conn_destino.port}/{conn_destino.schema}"
        engine_destino = sqlalchemy.create_engine(conn_str_destino)

        print("Leyendo datos desde vmrf_signing_control_workers...")
        df = pd.read_sql("SELECT start_date, end_date, DESC_OPERACION FROM vmrf_signing_control_workers WHERE start_date IS NOT NULL AND end_date IS NOT NULL", con=engine_origen)

        print("Calculando duración en minutos...")
        df["duracion_minutos"] = (df["end_date"] - df["start_date"]).dt.total_seconds() / 60.0

        print("Agrupando por operación...")
        resumen = df.groupby("DESC_OPERACION", as_index=False)["duracion_minutos"].mean().rename(columns={"duracion_minutos": "duracion_media_minutos"})

        print("Redondeando valores...")
        resumen["duracion_media_minutos"] = resumen["duracion_media_minutos"].round(2)

        print("Creando tabla 'duracion_media_operacion' si no existe y escribiendo resultados...")
        resumen.to_sql("duracion_media_operacion", con=engine_destino, if_exists="replace", index=False)

        print("===== FIN DAG =====")

    tarea_duracion_media = PythonOperator(
        task_id='calcular_duracion_media_por_operacion',
        python_callable=calcular_duracion_media_operacion
    )
