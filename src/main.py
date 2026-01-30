import time
import os
import sys

# Asegurar que 'src' esté en el path para poder importar utils correctamente
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.logger import get_logger

# Inicializamos el logger principal
logger = get_logger("Principal_ETL")

def ejecutar_proceso_etl():
    """
    Función principal que orquesta todo el pipeline ETL.
    Aquí se integran los desarrollos de las semanas 2, 3 y 4.
    """
    logger.info("Iniciando Proceso ETL Diario...")
    
    try:
        # 1. Configuración y Conexión
        logger.info("[Fase 1] Inicializando conexiones a base de datos...")
        # TODO: Integrar lógica de conexión (Semana 3/4)
        
        # 2. Extracción (Lectura)
        logger.info("[Fase 2] Leyendo datos desde fuentes CSV...")
        # TODO: Integrar lectura de CSVs (Semana 2)
        
        # 3. Transformación (Limpieza y Lógica)
        logger.info("[Fase 3] Limpiando, validando y transformando datos...")
        # TODO: Integrar limpieza y validaciones (Semana 2/3)
        
        # 4. Carga (Load)
        logger.info("[Fase 4] Guardando datos procesados en base de datos...")
        # TODO: Integrar inserción en BD (Semana 4)
        
        logger.info("Proceso ETL finalizado con éxito.")
        
    except Exception as e:
        logger.error(f"El proceso ETL falló: {e}")
        # Aquí se podría añadir el envío de un correo de alerta

if __name__ == "__main__":
    ejecutar_proceso_etl()
