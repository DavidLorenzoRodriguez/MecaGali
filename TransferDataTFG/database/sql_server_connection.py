import pyodbc
from config.config import SQL_SERVER_ORIGEN_CONFIG

def obtener_conexion_sql_server():
    """Devuelve una conexión a SQL Server según la configuración proporcionada. """
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={SQL_SERVER_ORIGEN_CONFIG['server']},{SQL_SERVER_ORIGEN_CONFIG['port']};"
        f"DATABASE={SQL_SERVER_ORIGEN_CONFIG['database']};"
        f"UID={SQL_SERVER_ORIGEN_CONFIG['username']};"
        f"PWD={SQL_SERVER_ORIGEN_CONFIG['password']};"
        "Encrypt=no;TrustServerCertificate=yes"
    )
    return pyodbc.connect(conn_str)