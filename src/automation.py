import schedule
import time
import sys
import os
import subprocess
from datetime import datetime

# Asegurar que 'src' esté en el path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.logger import get_logger

logger = get_logger("Programador_Automatico")

def tarea_programada():
    """
    Ejecuta el proceso principal ETL.
    Se lanza como un subproceso independiente para asegurar un entorno limpio en cada ejecución
    y evitar conflictos de memoria o estados residuales.
    """
    hora_actual = datetime.now()
    logger.info(f"Iniciando tarea programada a las {hora_actual}")
    
    # Ruta absoluta al script main.py
    main_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "main.py")
    
    try:
        # Ejecutamos main.py usando el mismo intérprete de Python actual
        resultado = subprocess.run([sys.executable, main_script], capture_output=True, text=True)
        
        if resultado.returncode == 0:
            logger.info("Tarea finalizada correctamente.")
            # Mostramos la salida del script principal en el log del programador
            logger.info(f"Salida del proceso:\n{resultado.stdout}")
        else:
            logger.error(f"La tarea falló con código de error {resultado.returncode}")
            logger.error(f"Detalle del error:\n{resultado.stderr}")
            
    except Exception as e:
        logger.error(f"Error crítico al intentar ejecutar la tarea: {e}")

def iniciar_programador():
    logger.info("Iniciando el Programador de Tareas Automático.")
    logger.info("Configuración: Ejecución diaria a las 15:00 horas.")
    
    # Programamos la ejecución todos los días a las 15:00
    schedule.every().day.at("15:00").do(tarea_programada)
    
    # Para pruebas rápidas: descomentar la siguiente línea para ejecutar cada minuto
    # schedule.every(1).minutes.do(tarea_programada) 
    
    logger.info("Esperando a la próxima ejecución programada...")
    
    # Bucle infinito que mantiene el script vivo comprobando la hora
    while True:
        schedule.run_pending()
        time.sleep(60) # Revisar cada minuto para no consumir CPU innecesariamente

if __name__ == "__main__":
    iniciar_programador()
