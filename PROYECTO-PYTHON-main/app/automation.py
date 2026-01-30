"""
Módulo de Automatización
========================

Proporciona funcionalidades para programar la ejecución
automática del proceso ETL.
"""

import schedule
import time
import sys
from datetime import datetime
from typing import Optional

try:
    from app import config
    from app.logger import get_logger
    from app.pipeline import ejecutar_etl_completo
except ImportError:
    import config
    from logger import get_logger
    from pipeline import ejecutar_etl_completo

logger = get_logger("automation")


class ETLScheduler:
    """
    Programador de tareas ETL.
    Permite ejecutar el proceso ETL de forma programada.
    """

    def __init__(self):
        self.logger = logger
        self.running = False

    def _tarea_programada(self):
        """Ejecuta el proceso ETL programado."""
        hora_actual = datetime.now()
        self.logger.info(f"Iniciando tarea programada a las {hora_actual}")

        try:
            resultados = ejecutar_etl_completo(cargar_a_bd=True)
            self.logger.info("Tarea programada completada correctamente")
            self.logger.info(f"Resultados: {resultados}")
        except Exception as e:
            self.logger.error(f"Error en tarea programada: {e}")

    def iniciar(self, hora: Optional[str] = None, intervalo_minutos: Optional[int] = None):
        """
        Inicia el programador de tareas.

        Args:
            hora: Hora de ejecución diaria (formato HH:MM)
            intervalo_minutos: Intervalo en minutos para ejecución repetida
        """
        self.logger.info("=" * 60)
        self.logger.info("INICIANDO PROGRAMADOR DE TAREAS ETL")
        self.logger.info("=" * 60)

        if intervalo_minutos:
            schedule.every(intervalo_minutos).minutes.do(self._tarea_programada)
            self.logger.info(f"Configurado: Ejecución cada {intervalo_minutos} minutos")
        else:
            hora_ejecucion = hora or config.SCHEDULE_TIME
            schedule.every().day.at(hora_ejecucion).do(self._tarea_programada)
            self.logger.info(f"Configurado: Ejecución diaria a las {hora_ejecucion}")

        self.logger.info("Esperando próxima ejecución...")
        self.logger.info("Presione Ctrl+C para detener")

        self.running = True
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(60)  # Revisar cada minuto
        except KeyboardInterrupt:
            self.logger.info("Programador detenido por el usuario")
            self.running = False

    def detener(self):
        """Detiene el programador."""
        self.running = False
        self.logger.info("Programador detenido")


def iniciar_automatizacion(hora: Optional[str] = None):
    """
    Inicia la automatización del proceso ETL.

    Args:
        hora: Hora de ejecución diaria (formato HH:MM)
    """
    scheduler = ETLScheduler()
    scheduler.iniciar(hora=hora)


def ejecutar_una_vez():
    """Ejecuta el proceso ETL una sola vez."""
    logger.info("Ejecutando proceso ETL...")
    resultados = ejecutar_etl_completo(cargar_a_bd=True)
    logger.info("Proceso completado")
    return resultados


if __name__ == "__main__":
    # Por defecto, ejecutar una vez
    if len(sys.argv) > 1 and sys.argv[1] == "--schedule":
        hora = sys.argv[2] if len(sys.argv) > 2 else None
        iniciar_automatizacion(hora)
    else:
        ejecutar_una_vez()
