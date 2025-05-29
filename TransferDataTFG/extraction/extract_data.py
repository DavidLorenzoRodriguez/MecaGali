from database.sql_server_connection import obtener_conexion_sql_server
import pandas as pd

def get_table_data_sql_server(table_name: str):
    """
    Extrae todos los datos de la tabla SQL Server.
    """

    conn = obtener_conexion_sql_server()

    query = f"SELECT * FROM {table_name};"

    df = pd.read_sql(query, conn)
    conn.close()

    return df