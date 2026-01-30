"""
Módulo de Utilidades
====================

Funciones auxiliares para el procesamiento ETL:
- Hashing de datos sensibles
- Enmascaramiento de tarjetas
- Lectura de archivos CSV
- Extracción de fechas
"""

import re
import io
import hashlib
from pathlib import Path
from typing import Optional, List, Tuple

import pandas as pd

try:
    from app import config
    from app.logger import get_logger
except ImportError:
    import config
    from logger import get_logger

logger = get_logger("utils")


def hash_con_salt(valor: str) -> Optional[str]:
    """
    Genera un hash SHA-256 de un valor con salt.

    Args:
        valor: Valor a hashear

    Returns:
        str: Hash del valor o None si el valor está vacío
    """
    if not valor:
        return None

    texto_completo = f"{config.HASH_SALT}{str(valor)}"
    return hashlib.sha256(texto_completo.encode("utf-8")).hexdigest()


def enmascarar_tarjeta(numero_tarjeta: str) -> Optional[str]:
    """
    Enmascara un número de tarjeta dejando solo los últimos 4 dígitos visibles.

    Args:
        numero_tarjeta: Número de tarjeta a enmascarar

    Returns:
        str: Número enmascarado (ej: XXXX-XXXX-XXXX-1234)
    """
    if not numero_tarjeta:
        return None

    digitos = "".join(filter(str.isdigit, str(numero_tarjeta)))

    if len(digitos) < 4:
        return "X" * len(digitos)

    ultimos_4 = digitos[-4:]
    enmascarado = "X" * (len(digitos) - 4) + ultimos_4

    # Formatear en grupos de 4
    grupos = [enmascarado[i : i + 4] for i in range(0, len(enmascarado), 4)]
    return "-".join(grupos)


def extraer_fecha_archivo(nombre_archivo: str) -> Optional[str]:
    """
    Extrae la fecha de un nombre de archivo.

    Args:
        nombre_archivo: Nombre del archivo (ej: Clientes-2026-01-19.csv)

    Returns:
        str: Fecha extraída (ej: 2026-01-19) o None
    """
    patron = r"(\d{4}-\d{2}-\d{2})"
    match = re.search(patron, nombre_archivo)
    return match.group(1) if match else None


def leer_csv_con_encoding(filepath: Path) -> Optional[pd.DataFrame]:
    """
    Lee un archivo CSV intentando diferentes encodings.

    Args:
        filepath: Ruta al archivo CSV

    Returns:
        DataFrame: Datos del archivo o None si hay error
    """
    logger.info(f"Leyendo archivo: {filepath.name}")

    # Primero, detectar si el archivo tiene líneas con comillas
    try:
        with open(filepath, "r", encoding=config.FILE_ENCODING_FALLBACK) as f:
            lines = f.readlines()

        # Limpiar comillas que envuelven cada línea
        if lines and lines[0].startswith('"') and lines[0].strip().endswith('"'):
            cleaned_lines = [line.strip().strip('"') + "\n" for line in lines]
            df = pd.read_csv(
                io.StringIO("".join(cleaned_lines)),
                sep=config.CSV_SEPARATOR,
                dtype=str,
                na_values=["", "NULL", "null", "None", "NA"],
            )
            logger.info(f"  ✓ Leídas {len(df)} filas (formato con comillas)")
            return df
    except Exception:
        pass

    # Intentar con UTF-8
    try:
        df = pd.read_csv(
            filepath,
            sep=config.CSV_SEPARATOR,
            encoding=config.FILE_ENCODING,
            dtype=str,
            na_values=["", "NULL", "null", "None", "NA"],
        )
        logger.info(f"  ✓ Leídas {len(df)} filas (encoding UTF-8)")
        return df
    except UnicodeDecodeError:
        pass

    # Intentar con Latin-1
    try:
        df = pd.read_csv(
            filepath,
            sep=config.CSV_SEPARATOR,
            encoding=config.FILE_ENCODING_FALLBACK,
            dtype=str,
            na_values=["", "NULL", "null", "None", "NA"],
        )
        logger.info(f"  ✓ Leídas {len(df)} filas (encoding Latin-1)")
        return df
    except Exception as e:
        logger.error(f"Error al leer {filepath.name}: {e}")
        return None


def detectar_archivos(directorio: Path, pattern: str) -> List[Path]:
    """
    Detecta archivos que coinciden con un patrón en un directorio.

    Args:
        directorio: Directorio donde buscar
        pattern: Patrón regex para filtrar archivos

    Returns:
        List[Path]: Lista de archivos encontrados
    """
    archivos_encontrados = []

    if not directorio.exists():
        logger.warning(f"El directorio {directorio} no existe")
        return archivos_encontrados

    for archivo in directorio.glob("*.csv"):
        if re.match(pattern, archivo.name):
            archivos_encontrados.append(archivo)
            logger.info(f"  ✓ Archivo encontrado: {archivo.name}")

    if not archivos_encontrados:
        logger.warning(f"No se encontraron archivos con patrón {pattern}")

    return archivos_encontrados


def guardar_csv(df: pd.DataFrame, filepath: Path) -> bool:
    """
    Guarda un DataFrame en un archivo CSV.

    Args:
        df: DataFrame a guardar
        filepath: Ruta del archivo destino

    Returns:
        bool: True si se guardó correctamente
    """
    try:
        df.to_csv(
            filepath, sep=config.CSV_SEPARATOR, index=False, encoding=config.FILE_ENCODING
        )
        logger.info(f"  ✓ Guardado: {filepath.name} ({len(df)} filas)")
        return True
    except Exception as e:
        logger.error(f"Error al guardar {filepath.name}: {e}")
        return False


def validar_campos_obligatorios(
    df: pd.DataFrame, campos: List[str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Separa filas válidas de inválidas según campos obligatorios.

    Args:
        df: DataFrame a validar
        campos: Lista de campos obligatorios

    Returns:
        Tuple[DataFrame, DataFrame]: (filas válidas, filas rechazadas)
    """
    mascara_valida = pd.Series([True] * len(df))
    motivos_rechazo = [""] * len(df)

    for campo in campos:
        if campo in df.columns:
            mascara_campo = df[campo].notna() & (df[campo] != "")
            mascara_invalida = ~mascara_campo

            if mascara_invalida.any():
                for idx in df[mascara_invalida].index:
                    if motivos_rechazo[idx]:
                        motivos_rechazo[idx] += f"; {campo} vacío"
                    else:
                        motivos_rechazo[idx] = f"{campo} vacío"
                mascara_valida &= mascara_campo

    df["motivo_rechazo"] = motivos_rechazo
    df_valido = df[mascara_valida].copy()
    df_rechazado = df[~mascara_valida].copy()

    if len(df_rechazado) > 0:
        logger.warning(f"  ⚠ {len(df_rechazado)} filas rechazadas por campos vacíos")

    return df_valido, df_rechazado
