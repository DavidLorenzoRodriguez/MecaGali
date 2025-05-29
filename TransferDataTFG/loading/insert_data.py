from database.mysql_connection import obtener_conexion_mysql
import logging
import pandas as pd
import math

logger = logging.getLogger(__name__)

def insert_data_mysql(table_name: str, data_df, primary_keys: list, batch_size=1000):
    """
    Inserta datos en una tabla MySQL por lotes para evitar pérdidas de conexión.
    Si los registros existen, actualiza según la clave primaria.
    """
    try:
        engine = obtener_conexion_mysql()

        # Convertir NaN a None CORRECCIÓN CLAVE
        data_df = data_df.where(pd.notnull(data_df), None)

        # Preparar columnas y placeholders
        columns = data_df.columns.tolist()
        placeholders = ", ".join(["%s"] * len(columns))
        columns_sql = ", ".join([f"`{col}`" for col in columns])

        # Excluir las columnas PK del UPDATE
        columns_to_update = [col for col in columns if col not in primary_keys]

        if not columns_to_update:
            logger.warning(f"No hay columnas para actualizar en '{table_name}' (todas son clave primaria). Solo se intentará insertar.")
            update_sql = ""
        else:
            update_sql = ", ".join([f"`{col}`=VALUES(`{col}`)" for col in columns_to_update])

        # Generar consulta final
        insert_query = f"""
        INSERT INTO `{table_name}` ({columns_sql})
        VALUES ({placeholders})
        {'ON DUPLICATE KEY UPDATE ' + update_sql if update_sql else ''};
        """

        total_rows = len(data_df)
        total_batches = math.ceil(total_rows / batch_size)

        logger.info(f"Iniciando inserción por lotes ({batch_size} filas por lote)")

       # Inserción en lotes pequeños
        conn = engine.raw_connection()
        cursor = conn.cursor()

        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        for i in range(total_batches):
            start = i * batch_size
            end = min(start + batch_size, total_rows)

            # Solución para NaN → None
            batch_data = (
                data_df.iloc[start:end]
                .astype(object)
                .where(pd.notnull(data_df), None)
                .values.tolist()
            )

            cursor.executemany(insert_query, batch_data)
            conn.commit()

            logger.info(f"Lote {i + 1}/{total_batches} insertado (filas {start} a {end}).")

        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        cursor.close()
        conn.close()


    except Exception as e:
        logger.error(f"Error al insertar/actualizar datos en la tabla '{table_name}': {e}")
