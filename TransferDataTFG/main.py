from extraction.extract_structure import get_table_structure_sql_server
from extraction.extract_data import get_table_data_sql_server
from extraction.extract_primary_key import get_primary_key_sql_server  # Importación de PK
from loading.create_table import create_table_mysql
from loading.insert_data import insert_data_mysql
from config.config import PRIMARY_KEYS_CONFIG  # Importar el diccionario de PK por tabla
from utils.logger import log  # Logger que usamos en todo


def procesar_tabla(table_name):
    """
    Procesa una única tabla: extrae estructura, datos y hace la inserción.
    """
    try:
        log(f"\n===== INICIO DEL PROCESO PARA LA TABLA '{table_name}' =====")

        # Paso 1: Obtener estructura de la tabla
        log(f"Iniciando extracción de estructura de la tabla '{table_name}' desde SQL Server.")
        estructura = get_table_structure_sql_server(table_name)
        log(f"Estructura obtenida correctamente para la tabla '{table_name}'.")
        log(f"Columnas encontradas: {estructura['COLUMN_NAME'].tolist()}")

        # Paso 2: Obtener PRIMARY KEY de SQL Server o desde configuración manual
        log(f"Extrayendo clave primaria de la tabla '{table_name}' desde SQL Server.")
        primary_keys = get_primary_key_sql_server(table_name)

        if not primary_keys:
            log(f"No se detectó PRIMARY KEY en SQL Server. Consultando configuración manual.")
            primary_keys = PRIMARY_KEYS_CONFIG.get(table_name, [])
            if not primary_keys:
                log(f"No se ha definido PRIMARY KEY para la tabla '{table_name}'. La inserción no podrá hacer actualizaciones.", level="WARNING")
            else:
                log(f"Primary key asignada desde configuración: {primary_keys}")
        else:
            log(f"Primary key detectada desde SQL Server: {primary_keys}")

        # Paso 3: Crear tabla en MySQL (si no existe) con PRIMARY KEY
        log(f"Iniciando creación de la tabla '{table_name}' en MySQL (si no existe).")
        create_table_mysql(table_name, estructura, primary_keys)
        log(f"Tabla '{table_name}' creada correctamente o ya existente en MySQL.")

        # Paso 4: Extraer datos de SQL Server
        log(f"Iniciando extracción de datos de la tabla '{table_name}' desde SQL Server.")
        datos = get_table_data_sql_server(table_name)
        log(f"Datos extraídos: {len(datos)} filas.")

        # Paso 5: Insertar o actualizar datos en MySQL
        log(f"Iniciando inserción/actualización de datos en la tabla '{table_name}' en MySQL.")
        insert_data_mysql(table_name, datos, primary_keys)
        log(f"Datos insertados/actualizados correctamente en la tabla '{table_name}'.")

        log(f"===== FIN DEL PROCESO PARA LA TABLA '{table_name}' =====\n")

    except Exception as e:
        log(f"Error durante el procesamiento de la tabla '{table_name}': {str(e)}", level="ERROR")


def main():
    """
    Proceso principal: pide al usuario las tablas y las procesa una a una.
    """
    # Pedimos al usuario las tablas a procesar
    tablas_input = input("Introduce las tablas separadas por coma: ")
    tablas = [tabla.strip() for tabla in tablas_input.split(",") if tabla.strip()]

    log("===== INICIO DEL PROCESO DE TRANSFERENCIA DE DATOS PARA MÚLTIPLES TABLAS =====")

    # Procesar cada tabla
    for table_name in tablas:
        procesar_tabla(table_name)

    log("===== FIN DEL PROCESO DE TODAS LAS TABLAS =====")


if __name__ == "__main__":
    main()
