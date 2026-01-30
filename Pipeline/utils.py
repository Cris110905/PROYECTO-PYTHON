import re
import unicodedata
import hashlib
import logging
from pathlib import Path
from datetime import datetime
from logging.handlers import RotatingFileHandler
import config


def setup_logger(name, log_file):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    if logger.handlers:
        logger.handlers.clear()
    
    handler = RotatingFileHandler(
        config.LOGS_DIR / log_file,
        maxBytes=config.LOG_MAX_BYTES,
        backupCount=config.LOG_BACKUP_COUNT,
        encoding='utf-8'
    )
    
    formatter = logging.Formatter(
        config.LOG_FORMAT,
        datefmt=config.LOG_DATE_FORMAT
    )
    handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


def eliminar_acentos(texto):
    if not isinstance(texto, str):
        return texto
    
    texto_nfd = unicodedata.normalize('NFD', texto)
    sin_acentos = ''.join(c for c in texto_nfd if not unicodedata.combining(c))
    return sin_acentos


def validar_dni(dni):
    if not isinstance(dni, str):
        return False
    
    dni = dni.strip().upper()
    
    patron = r'^\d{8}[A-Z]$'
    if not re.match(patron, dni):
        return False
    
    letras = 'TRWAGMYFPDXBNJZSQVHLCKE'
    numero = int(dni[:8])
    letra_calculada = letras[numero % 23]
    
    return dni[8] == letra_calculada


def validar_telefono(telefono):
    if not telefono:
        return False
    
    telefono_limpio = ''.join(filter(str.isdigit, str(telefono)))
    
    if len(telefono_limpio) != 9:
        return False
    
    return telefono_limpio[0] in ['6', '7', '8', '9']


def validar_correo(correo):
    if not isinstance(correo, str):
        return False
    
    patron = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(patron, correo.strip()))


def normalizar_telefono(telefono):
    if not telefono:
        return telefono
    return ''.join(filter(str.isdigit, str(telefono)))


def normalizar_dni(dni):
    if not isinstance(dni, str):
        return dni
    return dni.strip().replace(' ', '').replace('-', '').upper()


def hash_con_salt(valor):
    if not valor:
        return None
    
    texto_completo = f"{config.HASH_SALT}{str(valor)}"
    return hashlib.sha256(texto_completo.encode('utf-8')).hexdigest()


def enmascarar_tarjeta(numero_tarjeta):
    if not numero_tarjeta:
        return None
    
    digitos = ''.join(filter(str.isdigit, str(numero_tarjeta)))
    
    if len(digitos) < 4:
        return 'X' * len(digitos)
    
    ultimos_4 = digitos[-4:]
    enmascarado = 'X' * (len(digitos) - 4) + ultimos_4
    
    grupos = [enmascarado[i:i+4] for i in range(0, len(enmascarado), 4)]
    return '-'.join(grupos)


def extraer_fecha_archivo(nombre_archivo):
    patron = r'(\d{4}-\d{2}-\d{2})'
    match = re.search(patron, nombre_archivo)
    return match.group(1) if match else None


def validar_fecha_expiracion(fecha_exp):
    if not isinstance(fecha_exp, str):
        return False
    
    patron = r'^\d{4}-\d{2}$'
    if not re.match(patron, fecha_exp):
        return False
    
    try:
        año, mes = fecha_exp.split('-')
        mes_int = int(mes)
        return 1 <= mes_int <= 12
    except:
        return False
