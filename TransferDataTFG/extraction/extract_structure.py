from database.sql_server_connection import obtener_conexion_sql_server
import pandas as pd

def get_table_structure_sql_server(table_name: str):
    """
    Obtiene la estructura (columnas y tipos) de una tabla en SQL Server usando pyodbc.
    """
    conn = obtener_conexion_sql_server()

    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}';
    """

    result = pd.read_sql(query, conn)  # Correcto
    conn.close()  # Cerrar al finalizar

    return result
