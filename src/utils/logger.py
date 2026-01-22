"""
Sistema de Logging Centralizado para el Proyecto ETL
Autor: Cristian
Semana 4: Sistema de logging implementado
"""

import logging
import os
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from datetime import datetime
from pathlib import Path


class ETLLogger:
    """
    Clase para gestionar el sistema de logging del proyecto ETL.
    Proporciona logging a archivo y consola con rotación automática.
    """
    
    _instance = None
    _loggers = {}
    
    def __new__(cls):
        """Implementa patrón Singleton para tener una única instancia del logger"""
        if cls._instance is None:
            cls._instance = super(ETLLogger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Inicializa el sistema de logging"""
        if self._initialized:
            return
            
        self._initialized = True
        self.log_dir = self._setup_log_directory()
        self.log_format = self._get_log_format()
        self.date_format = '%Y-%m-%d %H:%M:%S'
        
    def _setup_log_directory(self) -> Path:
        """
        Crea el directorio de logs si no existe.
        
        Returns:
            Path: Ruta al directorio de logs
        """
        # Obtener la raíz del proyecto
        project_root = Path(__file__).parent.parent.parent
        log_dir = project_root / 'logs'
        
        # Crear directorio
        log_dir.mkdir(exist_ok=True)
        
        return log_dir
    
    def _get_log_format(self) -> str:
        """
        Define el formato de los mensajes de log.
        
        Returns:
            str: Formato del log
        """
        return '%(asctime)s | %(name)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(funcName)s() | %(message)s'
    
    def get_logger(self, name: str, level: str = 'INFO', 
                   console: bool = True, file: bool = True,
                   rotation_type: str = 'size') -> logging.Logger:
        """
        Obtiene o crea un logger configurado.
        
        Args:
            name: Nombre del logger (generalmente __name__ del módulo)
            level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            console: Si True, muestra logs en consola
            file: Si True, guarda logs en archivo
            rotation_type: Tipo de rotación ('size' o 'time')
            
        Returns:
            logging.Logger: Logger configurado
        """
        # Si el logger ya existe, devolverlo
        if name in self._loggers:
            return self._loggers[name]
        
        # Crear nuevo logger
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level.upper()))
        logger.propagate = False  # Evitar duplicación de logs
        
        # Limpiar handlers existentes
        logger.handlers.clear()
        
        # Crear formatter
        formatter = logging.Formatter(
            fmt=self.log_format,
            datefmt=self.date_format
        )
        
        # Handler para consola
        if console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # Handler para archivo
        if file:
            file_handler = self._create_file_handler(name, rotation_type)
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        # Guardar logger en caché
        self._loggers[name] = logger
        
        return logger
    
    def _create_file_handler(self, logger_name: str, rotation_type: str) -> logging.Handler:
        """
        Crea un handler de archivo con rotación.
        
        Args:
            logger_name: Nombre del logger
            rotation_type: Tipo de rotación ('size' o 'time')
            
        Returns:
            logging.Handler: Handler configurado
        """
        # Nombre del archivo de log
        log_filename = self.log_dir / f"{logger_name.replace('.', '_')}.log"
        
        if rotation_type == 'size':
            # Rotación por tamaño (10MB por archivo, mantener 5 backups)
            handler = RotatingFileHandler(
                filename=log_filename,
                maxBytes=10 * 1024 * 1024,  # 10 MB
                backupCount=5,
                encoding='utf-8'
            )
        elif rotation_type == 'time':
            # Rotación por tiempo (diaria, mantener 30 días)
            handler = TimedRotatingFileHandler(
                filename=log_filename,
                when='midnight',
                interval=1,
                backupCount=30,
                encoding='utf-8'
            )
        else:
            # Por defecto, usar rotación por tamaño
            handler = RotatingFileHandler(
                filename=log_filename,
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding='utf-8'
            )
        
        return handler
    
    def create_etl_logger(self, module_name: str) -> logging.Logger:
        """
        Crea un logger específico para módulos ETL.
        
        Args:
            module_name: Nombre del módulo ETL
            
        Returns:
            logging.Logger: Logger configurado para ETL
        """
        logger_name = f"etl.{module_name}"
        return self.get_logger(
            name=logger_name,
            level='INFO',
            console=True,
            file=True,
            rotation_type='time'  # Rotación diaria para ETL
        )
    
    def log_etl_process(self, logger: logging.Logger, process_name: str, 
                       records_processed: int = 0, status: str = 'START'):
        """
        Registra información sobre un proceso ETL.
        
        Args:
            logger: Logger a utilizar
            process_name: Nombre del proceso ETL
            records_processed: Número de registros procesados
            status: Estado del proceso (START, PROGRESS, SUCCESS, ERROR)
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        message = f"[{status}] Proceso: {process_name}"
        if records_processed > 0:
            message += f" | Registros procesados: {records_processed}"
        message += f" | Timestamp: {timestamp}"
        
        if status == 'ERROR':
            logger.error(message)
        elif status == 'SUCCESS':
            logger.info(message)
        elif status == 'START':
            logger.info(message)
        else:
            logger.debug(message)


# Función helper para obtener un logger fácilmente
def get_logger(name: str = None, level: str = 'INFO') -> logging.Logger:
    """
    Función helper para obtener un logger configurado.
    
    Args:
        name: Nombre del logger (si es None, usa 'etl')
        level: Nivel de logging
        
    Returns:
        logging.Logger: Logger configurado
    """
    if name is None:
        name = 'etl'
    
    etl_logger = ETLLogger()
    return etl_logger.get_logger(name, level)


# Función helper para crear logger ETL
def get_etl_logger(module_name: str) -> logging.Logger:
    """
    Función helper para crear un logger ETL.
    
    Args:
        module_name: Nombre del módulo
        
    Returns:
        logging.Logger: Logger configurado para ETL
    """
    etl_logger = ETLLogger()
    return etl_logger.create_etl_logger(module_name)
