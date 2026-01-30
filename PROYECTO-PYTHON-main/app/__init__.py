"""
Proyecto ETL - Sistema de procesamiento de datos de Clientes y Tarjetas
=====================================================================

Este módulo proporciona funcionalidades para:
- Extracción de datos desde archivos CSV (local o Dropbox)
- Transformación y limpieza de datos
- Validación de campos (DNI, teléfono, correo, tarjetas)
- Carga a base de datos PostgreSQL
- Automatización programada de procesos

Estructura del proyecto:
    app/
    ├── config.py          - Configuración centralizada
    ├── database.py        - Conexión y operaciones de BD
    ├── utils.py           - Funciones de utilidad
    ├── logger.py          - Sistema de logging
    ├── validators.py      - Validaciones de datos
    ├── normalizers.py     - Normalización de datos
    ├── pipeline.py        - Pipeline ETL principal
    └── automation.py      - Automatización programada
"""

__version__ = "1.0.0"
__author__ = "Equipo ETL"
