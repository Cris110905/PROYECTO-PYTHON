"""
Módulo de Validaciones
======================

Contiene todas las funciones de validación de datos:
- DNI español
- Teléfono español
- Correo electrónico
- Número de tarjeta
- Fecha de expiración
"""

import re
from typing import Optional

try:
    from app.logger import get_logger
except ImportError:
    from logger import get_logger

logger = get_logger("validators")


def validar_dni(dni: str) -> bool:
    """
    Valida un DNI español.

    Args:
        dni: DNI a validar

    Returns:
        bool: True si el DNI es válido
    """
    if not isinstance(dni, str):
        return False

    dni = dni.strip().upper()

    # Patrón: 8 dígitos + 1 letra
    patron = r"^\d{8}[A-Z]$"
    if not re.match(patron, dni):
        return False

    # Validar letra
    letras = "TRWAGMYFPDXBNJZSQVHLCKE"
    numero = int(dni[:8])
    letra_calculada = letras[numero % 23]

    return dni[8] == letra_calculada


def validar_telefono(telefono: str) -> bool:
    """
    Valida un teléfono español (móvil o fijo).

    Args:
        telefono: Teléfono a validar

    Returns:
        bool: True si el teléfono es válido
    """
    if not telefono:
        return False

    # Extraer solo dígitos
    telefono_limpio = "".join(filter(str.isdigit, str(telefono)))

    # Debe tener 9 dígitos
    if len(telefono_limpio) != 9:
        return False

    # Debe empezar por 6, 7, 8 o 9
    return telefono_limpio[0] in ["6", "7", "8", "9"]


def validar_correo(correo: str) -> bool:
    """
    Valida un correo electrónico.

    Args:
        correo: Correo a validar

    Returns:
        bool: True si el correo es válido
    """
    if not isinstance(correo, str):
        return False

    patron = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(patron, correo.strip()))


def validar_numero_tarjeta(numero: str) -> bool:
    """
    Valida un número de tarjeta de crédito usando el algoritmo de Luhn.

    Args:
        numero: Número de tarjeta a validar

    Returns:
        bool: True si el número es válido
    """
    if not numero:
        return False

    # Extraer solo dígitos
    digitos = "".join(filter(str.isdigit, str(numero)))

    # Debe tener entre 13 y 19 dígitos
    if len(digitos) < 13 or len(digitos) > 19:
        return False

    # Algoritmo de Luhn
    suma = 0
    num_digitos = len(digitos)
    paridad = num_digitos % 2

    for i, digito in enumerate(digitos):
        d = int(digito)
        if i % 2 == paridad:
            d *= 2
            if d > 9:
                d -= 9
        suma += d

    return suma % 10 == 0


def validar_cvv(cvv: str) -> bool:
    """
    Valida un código CVV de tarjeta.

    Args:
        cvv: CVV a validar

    Returns:
        bool: True si el CVV es válido (3-4 dígitos)
    """
    if not cvv:
        return False

    cvv_limpio = "".join(filter(str.isdigit, str(cvv)))
    return len(cvv_limpio) in [3, 4]


def validar_fecha_expiracion(fecha_exp: str) -> bool:
    """
    Valida una fecha de expiración de tarjeta.

    Formatos aceptados: YYYY-MM, MM/YY, MM/YYYY

    Args:
        fecha_exp: Fecha de expiración a validar

    Returns:
        bool: True si la fecha es válida y no ha expirado
    """
    if not isinstance(fecha_exp, str):
        return False

    fecha_exp = fecha_exp.strip()

    # Formato YYYY-MM
    patron_iso = r"^\d{4}-\d{2}$"
    if re.match(patron_iso, fecha_exp):
        try:
            año, mes = fecha_exp.split("-")
            mes_int = int(mes)
            return 1 <= mes_int <= 12
        except:
            return False

    # Formato MM/YY o MM/YYYY
    patron_slash = r"^\d{2}/\d{2,4}$"
    if re.match(patron_slash, fecha_exp):
        try:
            mes, año = fecha_exp.split("/")
            mes_int = int(mes)
            return 1 <= mes_int <= 12
        except:
            return False

    return False


def validar_nombre(nombre: str) -> bool:
    """
    Valida que un nombre solo contenga caracteres válidos.

    Args:
        nombre: Nombre a validar

    Returns:
        bool: True si el nombre es válido
    """
    if not isinstance(nombre, str) or not nombre.strip():
        return False

    # Permite letras (incluyendo acentos), espacios y guiones
    patron = r"^[a-zA-ZáéíóúÁÉÍÓÚñÑüÜ\s\-]+$"
    return bool(re.fullmatch(patron, nombre.strip()))


def validar_cod_cliente(cod: str) -> bool:
    """
    Valida un código de cliente.

    Args:
        cod: Código de cliente a validar

    Returns:
        bool: True si el código es válido
    """
    if not isinstance(cod, str) or not cod.strip():
        return False

    # Formato esperado: letra(s) seguida(s) de números (ej: C001, CLI123)
    patron = r"^[A-Z]+\d+$"
    return bool(re.match(patron, cod.strip().upper()))
