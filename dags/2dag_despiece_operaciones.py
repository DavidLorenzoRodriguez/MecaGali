from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import pandas as pd
import sqlalchemy
import sys

sys.path.append('/opt/airflow/include')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='2dag_despiece_operaciones',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Asociación de operaciones a artículos padres e hijos',
) as dag:

    def relacionar_operaciones():
        try:
            print("Conectando a bases de datos...")
            conn_transf = BaseHook.get_connection("mariadb_mecagali_transf")
            conn_str_transf = f"mysql+pymysql://{conn_transf.login}:{conn_transf.password}@{conn_transf.host}:{conn_transf.port}/{conn_transf.schema}"
            engine_transf = sqlalchemy.create_engine(conn_str_transf)

            conn_origen = BaseHook.get_connection("mariadb_mecagali")
            conn_str_origen = f"mysql+pymysql://{conn_origen.login}:{conn_origen.password}@{conn_origen.host}:{conn_origen.port}/{conn_origen.schema}"
            engine_origen = sqlalchemy.create_engine(conn_str_origen)
            print("Conexión establecida")

            # Cargar jerarquía de artículos (padres e hijos)
            query_jerarquia = "SELECT * FROM jerarquia_articulos"
            df_jerarquia = pd.read_sql(query_jerarquia, engine_transf)

            # Cargar operaciones de fabricación
            query_operaciones = "SELECT * FROM vmrf_prod_operaciones_despiece"
            df_operaciones = pd.read_sql(query_operaciones, engine_origen)

            print("Datos cargados correctamente")

            # Relacionar operaciones con artículos PADRES
            df_oper_padre = pd.merge(
                df_jerarquia,
                df_operaciones,
                how="left",
                left_on="ID_VERSION_ARTICULO",
                right_on="ID_VERSION_ARTICULO",
                suffixes=('_jerarquia', '_op')
            )
            df_oper_padre["CODIGO_INTERNO_FINAL"] = df_oper_padre["CODIGO_INTERNO"]
            df_oper_padre["DESC_ARTICULO_FINAL"] = df_oper_padre["DESC_ARTICULO"]

            # Relacionar operaciones con artículos HIJOS
            df_oper_hijo = pd.merge(
                df_jerarquia,
                df_operaciones,
                how="left",
                left_on="VER_ARTICULO_HIJO",
                right_on="ID_VERSION_ARTICULO",
                suffixes=('_jerarquia', '_op')
            )

            # Usar el nombre correcto de columna de operaciones si aparecen repetidas
            cod_interno_cols = [col for col in df_oper_hijo.columns if col.startswith("CODIGO_INTERNO")]
            desc_articulo_cols = [col for col in df_oper_hijo.columns if col.startswith("DESC_ARTICULO")]
            cod_interno_col = "CODIGO_INTERNO"
            desc_articulo_col = "DESC_ARTICULO"
            if len(cod_interno_cols) > 1:
                cod_interno_col = [c for c in cod_interno_cols if c != "CODIGO_INTERNO"][0]
            if len(desc_articulo_cols) > 1:
                desc_articulo_col = [c for c in desc_articulo_cols if c != "DESC_ARTICULO"][0]

            df_oper_hijo["CODIGO_INTERNO_FINAL"] = df_oper_hijo[cod_interno_col].fillna(df_oper_hijo["CODIGO_INTERNO"])
            df_oper_hijo["DESC_ARTICULO_FINAL"] = df_oper_hijo[desc_articulo_col].fillna(df_oper_hijo["DESC_ARTICULO"])

            # Concatenar resultados
            df_final = pd.concat([df_oper_padre, df_oper_hijo], ignore_index=True)

            # Reemplazar nulos/cero en TP_TRABAJADOR y TP_MAQUINA por 1
            for col in ['TP_TRABAJADOR', 'TP_MAQUINA']:
                df_final[col] = pd.to_numeric(df_final[col], errors='coerce').fillna(1)
                df_final.loc[df_final[col] == 0, col] = 1

            # Reemplazar nulos/cero en optimal_setup_quantity por 1000
            df_final["optimal_setup_quantity"] = pd.to_numeric(df_final["optimal_setup_quantity"], errors='coerce').fillna(1000)
            df_final.loc[df_final["optimal_setup_quantity"] == 0, "optimal_setup_quantity"] = 1000

            columnas = [
                "VER_ARTICULO_PADRE", "VER_ARTICULO_HIJO", "CODIGO_INTERNO_FINAL", "DESC_ARTICULO_FINAL",
                "Nivel", "Tipo", "DESC_OPERACION", "TIEMPO_FIJO", "TIEMPO_VARIABLE",
                "TP_TRABAJADOR", "TP_MAQUINA", "optimal_setup_quantity"
            ]
            df_final = df_final[columnas]
            df_final = df_final.rename(columns={
                "CODIGO_INTERNO_FINAL": "CODIGO_INTERNO",
                "DESC_ARTICULO_FINAL": "DESC_ARTICULO"
            })

            print("Relación de operaciones generada correctamente")

            # Guardar el resultado en la tabla de operaciones relacionadas
            df_final.to_sql("operaciones_relacionadas", con=engine_transf, if_exists="replace", index=False)

            print("Operaciones relacionadas guardadas correctamente en la base de datos")

        except Exception as e:
            print(f"Error al relacionar operaciones: {e}")
            raise

    # Definir la tarea del DAG
    tarea_relacion_operaciones = PythonOperator(
        task_id='relacionar_operaciones',
        python_callable=relacionar_operaciones
    )
