from sqlalchemy import create_engine
from config.config import MYSQL_CONFIG

def obtener_conexion_mysql():
    """ Devuelve una conexión a MySQL usando SQLAlchemy. """
    engine = create_engine(
        f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@"
        f"{MYSQL_CONFIG['host']}/{MYSQL_CONFIG['database']}",
        echo=True
    )
    return engine
