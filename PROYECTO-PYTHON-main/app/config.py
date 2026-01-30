"""
Configuración centralizada del proyecto ETL
===========================================

Este módulo contiene todas las configuraciones del proyecto,
incluyendo rutas, conexiones de base de datos, patrones de archivos
y configuraciones de logging.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# =============================================================================
# RUTAS DEL PROYECTO
# =============================================================================

# Directorio base del proyecto (un nivel arriba de app/)
BASE_DIR = Path(__file__).resolve().parent.parent

# Directorios de datos
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
ERRORS_DIR = DATA_DIR / "errors"
LOGS_DIR = BASE_DIR / "logs"

# Directorio de ficheros originales (compatibilidad con estructura anterior)
FICHEROS_DIR = BASE_DIR / "ficheros"

# Crear directorios si no existen
for directory in [DATA_DIR, INPUT_DIR, OUTPUT_DIR, ERRORS_DIR, LOGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# =============================================================================
# CONFIGURACIÓN DE BASE DE DATOS
# =============================================================================

DB_CONFIG = {
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "2020"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "Clientes"),
}

# String de conexión para SQLAlchemy
DATABASE_URL = (
    f"postgresql+pg8000://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# =============================================================================
# CONFIGURACIÓN DE DROPBOX
# =============================================================================

DROPBOX_TOKEN = os.getenv("DROPBOX_TOKEN", "")

# =============================================================================
# CONFIGURACIÓN DE ARCHIVOS CSV
# =============================================================================

FILE_ENCODING = "utf-8"
FILE_ENCODING_FALLBACK = "latin-1"
CSV_SEPARATOR = ";"

# Patrones para detectar archivos
CLIENTES_PATTERN = r"Clientes-\d{4}-\d{2}-\d{2}\.csv"
TARJETAS_PATTERN = r"Tarjetas-\d{4}-\d{2}-\d{2}\.csv"

# =============================================================================
# CONFIGURACIÓN DE SEGURIDAD
# =============================================================================

# Salt para hashing de datos sensibles
HASH_SALT = os.getenv("ETL_HASH_SALT", "proyecto_etl_salt_secret_2026")

# =============================================================================
# CONFIGURACIÓN DE LOGGING
# =============================================================================

LOG_FORMAT = "%(asctime)s | %(name)s | %(levelname)-8s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
LOG_BACKUP_COUNT = 5

# =============================================================================
# CONFIGURACIÓN DE AUTOMATIZACIÓN
# =============================================================================

# Hora de ejecución automática (formato 24h)
SCHEDULE_TIME = os.getenv("SCHEDULE_TIME", "15:00")

# =============================================================================
# ESQUEMAS DE TABLAS DE BASE DE DATOS
# =============================================================================

TABLES_SCHEMA = {
    "clients": {
        "columns": [
            ("cod_cliente", "VARCHAR(20) PRIMARY KEY"),
            ("nombre", "VARCHAR(100)"),
            ("apellido1", "VARCHAR(100)"),
            ("apellido2", "VARCHAR(100)"),
            ("dni", "VARCHAR(20) UNIQUE"),
            ("dni_hash", "VARCHAR(64)"),
            ("correo", "VARCHAR(150)"),
            ("telefono", "VARCHAR(20)"),
            ("fecha_procesado", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ]
    },
    "tarjetas": {
        "columns": [
            ("id", "SERIAL PRIMARY KEY"),
            ("cod_cliente", "VARCHAR(20) REFERENCES clients(cod_cliente)"),
            ("numero_tarjeta_hash", "VARCHAR(64)"),
            ("numero_tarjeta_masked", "VARCHAR(25)"),
            ("fecha_exp", "VARCHAR(10)"),
            ("cvv_hash", "VARCHAR(64)"),
            ("fecha_procesado", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"),
        ]
    },
}

# =============================================================================
# CAMPOS OBLIGATORIOS POR TIPO DE ARCHIVO
# =============================================================================

REQUIRED_FIELDS = {
    "clientes": ["cod_cliente", "nombre", "correo"],
    "tarjetas": ["cod_cliente", "numero_tarjeta"],
}
