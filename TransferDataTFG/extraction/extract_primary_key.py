from database.sql_server_connection import obtener_conexion_sql_server
import pandas as pd

def get_primary_key_sql_server(table_name: str):
    """
    Devuelve la lista de columnas que forman la PRIMARY KEY de la tabla indicada.
    """
    conn = obtener_conexion_sql_server()

    query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
    AND TABLE_NAME = '{table_name}';
    """

    result = pd.read_sql(query, conn)
    conn.close()

    return result['COLUMN_NAME'].tolist()  # Lista de columnas
