from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import pandas as pd
import sqlalchemy

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='3dag_resumen_tiempo_fabricacion',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Resumen de tiempo de fabricación por artículo padre',
) as dag:

    def generar_resumen_tiempo():
        try:
            print("Conectando a base de datos...")
            conn_transf = BaseHook.get_connection("mariadb_mecagali_transf")
            conn_str_transf = f"mysql+pymysql://{conn_transf.login}:{conn_transf.password}@{conn_transf.host}:{conn_transf.port}/{conn_transf.schema}"
            engine = sqlalchemy.create_engine(conn_str_transf)

            # Leer la tabla de operaciones relacionadas
            df = pd.read_sql("SELECT * FROM operaciones_relacionadas", con=engine)

            # Excluir filas con CODIGO_INTERNO no válidos
            df = df[(df["CODIGO_INTERNO"].notna()) & 
                    (df["CODIGO_INTERNO"] != 'N/A') & 
                    (df["CODIGO_INTERNO"].str.strip() != '')]

            # Asegurarse de no tener nulos y convertir a número
            for col in ['TIEMPO_FIJO', 'TIEMPO_VARIABLE', 'TP_TRABAJADOR', 'TP_MAQUINA', 'optimal_setup_quantity']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            df["optimal_setup_quantity"] = df["optimal_setup_quantity"].replace(0, 1)

            # Calcular el tiempo de cada operación según la fórmula
            df["TIEMPO_OP"] = ((df["TIEMPO_FIJO"] + df["TIEMPO_VARIABLE"]) * df["TP_TRABAJADOR"] * df["TP_MAQUINA"]) / df["optimal_setup_quantity"]

            # Agrupar por artículo padre (CODIGO_INTERNO), sumar los tiempos y mantener la descripción
            resumen = df.groupby("CODIGO_INTERNO").agg({
                "DESC_ARTICULO": "first",
                "TIEMPO_OP": "sum"
            }).reset_index().rename(columns={
                "CODIGO_INTERNO": "CODIGO_PADRE",
                "TIEMPO_OP": "TIEMPO_TOTAL"
            })

            # Guardar en nueva tabla
            resumen.to_sql("tiempo_fabricacion_resumen", con=engine, if_exists="replace", index=False)

            print("Resumen de tiempos guardado correctamente")

        except Exception as e:
            print(f"Error al generar el resumen: {e}")
            raise

    tarea_resumen = PythonOperator(
        task_id='generar_resumen_tiempo',
        python_callable=generar_resumen_tiempo
    )
