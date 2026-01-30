import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
ERRORS_DIR = BASE_DIR / "errors"
LOGS_DIR = BASE_DIR / "logs"

for directory in [INPUT_DIR, OUTPUT_DIR, ERRORS_DIR, LOGS_DIR]:
    directory.mkdir(exist_ok=True)

FILE_ENCODING = 'utf-8'
CSV_SEPARATOR = ';'

CLIENTES_PATTERN = r'Clientes-\d{4}-\d{2}-\d{2}\.csv'
TARJETAS_PATTERN = r'Tarjetas-\d{4}-\d{2}-\d{2}\.csv'

HASH_SALT = os.getenv('ETL_HASH_SALT', 'proyecto_etl_salt_secret')

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 5
