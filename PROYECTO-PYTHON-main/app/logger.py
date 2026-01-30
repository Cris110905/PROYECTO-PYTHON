"""
Sistema de Logging Centralizado
===============================

Proporciona un sistema de logging unificado con:
- Logging a archivo con rotación automática
- Logging a consola
- Formato consistente
- Patrón Singleton para gestión centralizada
"""

import logging
import sys
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from pathlib import Path
from datetime import datetime

# Importar configuración
try:
    from app import config
except ImportError:
    import config


class ETLLogger:
    """
    Gestor centralizado de logging para el proyecto ETL.
    Implementa patrón Singleton para asegurar consistencia.
    """

    _instance = None
    _loggers = {}

    def __new__(cls):
        """Implementa patrón Singleton"""
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Inicializa el sistema de logging"""
        if self._initialized:
            return

        self._initialized = True
        self.log_dir = config.LOGS_DIR
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def get_logger(
        self,
        name: str,
        level: str = "INFO",
        console: bool = True,
        file: bool = True,
        rotation_type: str = "size",
    ) -> logging.Logger:
        """
        Obtiene o crea un logger configurado.

        Args:
            name: Nombre del logger
            level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            console: Si True, muestra logs en consola
            file: Si True, guarda logs en archivo
            rotation_type: Tipo de rotación ('size' o 'time')

        Returns:
            logging.Logger: Logger configurado
        """
        if name in self._loggers:
            return self._loggers[name]

        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level.upper()))
        logger.propagate = False
        logger.handlers.clear()

        formatter = logging.Formatter(
            fmt=config.LOG_FORMAT, datefmt=config.LOG_DATE_FORMAT
        )

        # Handler para consola
        if console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        # Handler para archivo
        if file:
            file_handler = self._create_file_handler(name, rotation_type)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        self._loggers[name] = logger
        return logger

    def _create_file_handler(
        self, logger_name: str, rotation_type: str
    ) -> logging.Handler:
        """
        Crea un handler de archivo con rotación.

        Args:
            logger_name: Nombre del logger
            rotation_type: Tipo de rotación ('size' o 'time')

        Returns:
            logging.Handler: Handler configurado
        """
        log_filename = self.log_dir / f"{logger_name.replace('.', '_')}.log"

        if rotation_type == "time":
            handler = TimedRotatingFileHandler(
                filename=log_filename,
                when="midnight",
                interval=1,
                backupCount=30,
                encoding="utf-8",
            )
        else:  # size
            handler = RotatingFileHandler(
                filename=log_filename,
                maxBytes=config.LOG_MAX_BYTES,
                backupCount=config.LOG_BACKUP_COUNT,
                encoding="utf-8",
            )

        return handler


# =============================================================================
# FUNCIONES HELPER
# =============================================================================


def get_logger(name: str = "etl", level: str = "INFO") -> logging.Logger:
    """
    Función helper para obtener un logger configurado.

    Args:
        name: Nombre del logger
        level: Nivel de logging

    Returns:
        logging.Logger: Logger configurado
    """
    etl_logger = ETLLogger()
    return etl_logger.get_logger(name, level)


def get_pipeline_logger(pipeline_name: str) -> logging.Logger:
    """
    Crea un logger específico para un pipeline.

    Args:
        pipeline_name: Nombre del pipeline

    Returns:
        logging.Logger: Logger configurado
    """
    return get_logger(f"pipeline.{pipeline_name}", "INFO")
