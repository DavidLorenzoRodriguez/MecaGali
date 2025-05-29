import os
from datetime import datetime

LOG_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs', 'proceso.log')

def log(message: str, level: str = "INFO"):
    """
    Escribe un mensaje en el archivo de log.
    """
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"[{now}] [{level}] {message}\n"

    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)  # Crea carpeta 'logs' si no existe
    with open(LOG_FILE, "a", encoding='utf-8') as file:
        file.write(log_message)

    print(log_message.strip())  # Imprimir también en consola (opcional)
