"""
Módulo de Normalización
=======================

Contiene funciones para normalizar datos:
- Texto (nombres, apellidos)
- DNI
- Teléfono
- Correo
- Tarjetas
"""

import re
import unicodedata
from typing import Optional

try:
    from app.logger import get_logger
except ImportError:
    from logger import get_logger

logger = get_logger("normalizers")


def eliminar_acentos(texto: str) -> str:
    """
    Elimina acentos de un texto.

    Args:
        texto: Texto con posibles acentos

    Returns:
        str: Texto sin acentos
    """
    if not isinstance(texto, str):
        return texto

    texto_nfd = unicodedata.normalize("NFD", texto)
    sin_acentos = "".join(c for c in texto_nfd if not unicodedata.combining(c))
    return sin_acentos


def normalizar_texto(valor: str) -> str:
    """
    Normaliza un texto: elimina espacios y aplica Title Case.

    Args:
        valor: Texto a normalizar

    Returns:
        str: Texto normalizado
    """
    if not isinstance(valor, str):
        return str(valor).strip() if valor else ""

    return valor.strip().title()


def normalizar_texto_mayusculas(valor: str) -> str:
    """
    Normaliza un texto a mayúsculas.

    Args:
        valor: Texto a normalizar

    Returns:
        str: Texto en mayúsculas
    """
    if not isinstance(valor, str):
        return str(valor).strip().upper() if valor else ""

    return eliminar_acentos(valor.strip().upper())


def normalizar_dni(dni: str) -> str:
    """
    Normaliza un DNI: elimina espacios, guiones y convierte a mayúsculas.

    Args:
        dni: DNI a normalizar

    Returns:
        str: DNI normalizado
    """
    if not isinstance(dni, str):
        return str(dni) if dni else ""

    return dni.strip().replace(" ", "").replace("-", "").upper()


def normalizar_correo(correo: str) -> str:
    """
    Normaliza un correo electrónico a minúsculas.

    Args:
        correo: Correo a normalizar

    Returns:
        str: Correo normalizado
    """
    if not isinstance(correo, str):
        return str(correo).strip().lower() if correo else ""

    return correo.strip().lower()


def normalizar_telefono(telefono: str) -> str:
    """
    Normaliza un teléfono: extrae solo los dígitos.

    Args:
        telefono: Teléfono a normalizar

    Returns:
        str: Teléfono con solo dígitos
    """
    if not telefono:
        return ""

    return re.sub(r"\D", "", str(telefono))


def normalizar_numero_tarjeta(numero: str) -> str:
    """
    Normaliza un número de tarjeta: elimina espacios y guiones.

    Args:
        numero: Número de tarjeta a normalizar

    Returns:
        str: Número con solo dígitos
    """
    if not numero:
        return ""

    return "".join(filter(str.isdigit, str(numero)))


def normalizar_cvv(cvv: str) -> str:
    """
    Normaliza un CVV: extrae solo los dígitos.

    Args:
        cvv: CVV a normalizar

    Returns:
        str: CVV con solo dígitos
    """
    if not cvv:
        return ""

    return "".join(filter(str.isdigit, str(cvv)))


def normalizar_columnas_dataframe(df):
    """
    Normaliza los nombres de las columnas de un DataFrame.

    Args:
        df: DataFrame a normalizar

    Returns:
        DataFrame: DataFrame con columnas normalizadas
    """
    nuevos_nombres = {}
    for col in df.columns:
        nuevo_nombre = col.strip().lower()
        nuevo_nombre = eliminar_acentos(nuevo_nombre)
        nuevo_nombre = nuevo_nombre.replace(" ", "_")
        nuevos_nombres[col] = nuevo_nombre

    df.rename(columns=nuevos_nombres, inplace=True)
    return df


def limpiar_espacios_dataframe(df):
    """
    Elimina espacios en blanco de todas las columnas de texto.

    Args:
        df: DataFrame a limpiar

    Returns:
        DataFrame: DataFrame con espacios eliminados
    """
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].str.strip()

    return df


# =============================================================================
# DICCIONARIOS DE NORMALIZADORES POR TIPO
# =============================================================================

CLIENTES_NORMALIZERS = {
    "nombre": lambda x: eliminar_acentos(normalizar_texto(x)),
    "apellido1": normalizar_texto_mayusculas,
    "apellido2": normalizar_texto_mayusculas,
    "dni": normalizar_dni,
    "correo": normalizar_correo,
    "telefono": normalizar_telefono,
}

TARJETAS_NORMALIZERS = {
    "numero_tarjeta": normalizar_numero_tarjeta,
    "cvv": normalizar_cvv,
    "fecha_exp": lambda x: str(x).strip() if x else "",
}
