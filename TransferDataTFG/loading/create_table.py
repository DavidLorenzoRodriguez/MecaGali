from database.mysql_connection import obtener_conexion_mysql
from sqlalchemy import text
from transformation.map_types import map_sqlserver_to_mysql
import logging

logger = logging.getLogger(__name__)

def create_table_mysql(table_name: str, structure_df, primary_keys=None):
    """
    Crea una tabla en MySQL a partir de la estructura proporcionada,
    incluyendo PRIMARY KEY si se indica.
    
    :param table_name: Nombre de la tabla a crear.
    :param structure_df: DataFrame con la estructura (columnas y tipos).
    :param primary_keys: Lista de columnas que forman la clave primaria (opcional).
    """
    try:
        engine = obtener_conexion_mysql()
        columns = []

        # Generar definición de columnas
        for _, row in structure_df.iterrows():
            column_name = row['COLUMN_NAME']
            data_type = map_sqlserver_to_mysql(row['DATA_TYPE'], row['CHARACTER_MAXIMUM_LENGTH'])
            columns.append(f"`{column_name}` {data_type}")

        # Unir las columnas
        columns_sql = ", ".join(columns)

        # Preparar parte de PRIMARY KEY si se proporcionó
        pk_sql = ""
        if primary_keys:
            pk_sql = f", PRIMARY KEY ({', '.join([f'`{pk}`' for pk in primary_keys])})"

        # Generar consulta final
        create_query = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {columns_sql}
            {pk_sql}
        );
        """

        logger.info(f"Query de creación de tabla: {create_query}")  # Log del SQL

        # Ejecutar la query
        with engine.connect() as connection:
            connection.execute(text(create_query))

        logger.info(f"Tabla '{table_name}' creada o existente en MySQL, incluyendo PRIMARY KEY: {primary_keys if primary_keys else 'Sin PK especificada'}.")

    except Exception as e:
        logger.error(f"Error al crear la tabla '{table_name}': {e}")
