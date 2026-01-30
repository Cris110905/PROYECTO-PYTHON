#!/usr/bin/env python
"""
Proyecto ETL - Sistema de Procesamiento de Clientes y Tarjetas
==============================================================

Punto de entrada principal del proyecto.

USO:
    python run.py                    # Ejecutar ETL completo
    python run.py --pipeline         # Solo ejecutar pipelines (sin BD)
    python run.py --clientes         # Solo procesar clientes
    python run.py --tarjetas         # Solo procesar tarjetas
    python run.py --schedule         # Iniciar automatización
    python run.py --schedule 09:00   # Automatización a hora específica
    python run.py --create-tables    # Solo crear tablas en BD
    python run.py --test-db          # Probar conexión a BD
    python run.py --help             # Mostrar ayuda
"""

import sys
import argparse
from pathlib import Path

# Añadir el directorio actual al path
sys.path.insert(0, str(Path(__file__).parent))

from app.logger import get_logger
from app.pipeline import (
    ejecutar_etl_completo,
    ejecutar_pipeline_clientes,
    ejecutar_pipeline_tarjetas,
)
from app.database import get_database
from app.automation import iniciar_automatizacion

logger = get_logger("main")


def mostrar_banner():
    """Muestra el banner del proyecto."""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║   ███████╗████████╗██╗         ██████╗ ██╗██████╗ ███████╗  ║
║   ██╔════╝╚══██╔══╝██║         ██╔══██╗██║██╔══██╗██╔════╝  ║
║   █████╗     ██║   ██║         ██████╔╝██║██████╔╝█████╗    ║
║   ██╔══╝     ██║   ██║         ██╔═══╝ ██║██╔═══╝ ██╔══╝    ║
║   ███████╗   ██║   ███████╗    ██║     ██║██║     ███████╗  ║
║   ╚══════╝   ╚═╝   ╚══════╝    ╚═╝     ╚═╝╚═╝     ╚══════╝  ║
║                                                              ║
║   Sistema de Procesamiento de Clientes y Tarjetas            ║
║   Versión 1.0.0                                              ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
"""
    print(banner)


def crear_parser():
    """Crea el parser de argumentos."""
    parser = argparse.ArgumentParser(
        description="Sistema ETL para procesamiento de Clientes y Tarjetas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  python run.py                    Ejecutar ETL completo (pipelines + carga a BD)
  python run.py --pipeline         Solo ejecutar pipelines (sin cargar a BD)
  python run.py --clientes         Solo procesar archivos de clientes
  python run.py --tarjetas         Solo procesar archivos de tarjetas
  python run.py --schedule         Iniciar ejecución programada (15:00 por defecto)
  python run.py --schedule 09:00   Iniciar ejecución programada a las 09:00
  python run.py --create-tables    Crear tablas en la base de datos
  python run.py --test-db          Probar conexión a la base de datos
""",
    )

    parser.add_argument(
        "--pipeline",
        action="store_true",
        help="Ejecutar solo los pipelines (sin cargar a BD)",
    )

    parser.add_argument(
        "--clientes",
        action="store_true",
        help="Procesar solo archivos de clientes",
    )

    parser.add_argument(
        "--tarjetas",
        action="store_true",
        help="Procesar solo archivos de tarjetas",
    )

    parser.add_argument(
        "--schedule",
        nargs="?",
        const="15:00",
        metavar="HH:MM",
        help="Iniciar ejecución programada (por defecto a las 15:00)",
    )

    parser.add_argument(
        "--create-tables",
        action="store_true",
        help="Crear tablas en la base de datos",
    )

    parser.add_argument(
        "--test-db",
        action="store_true",
        help="Probar conexión a la base de datos",
    )

    parser.add_argument(
        "--no-banner",
        action="store_true",
        help="No mostrar el banner inicial",
    )

    return parser


def main():
    """Función principal."""
    parser = crear_parser()
    args = parser.parse_args()

    if not args.no_banner:
        mostrar_banner()

    try:
        # Probar conexión a BD
        if args.test_db:
            logger.info("Probando conexión a base de datos...")
            db = get_database()
            if db.test_connection():
                print("\n✓ Conexión a base de datos exitosa")
            else:
                print("\n✗ No se pudo conectar a la base de datos")
            return

        # Crear tablas
        if args.create_tables:
            logger.info("Creando tablas en la base de datos...")
            db = get_database()
            db.create_tables()
            print("\n✓ Tablas creadas correctamente")
            return

        # Ejecución programada
        if args.schedule:
            logger.info(f"Iniciando automatización a las {args.schedule}...")
            iniciar_automatizacion(hora=args.schedule)
            return

        # Solo clientes
        if args.clientes:
            logger.info("Ejecutando pipeline de clientes...")
            resultados = ejecutar_pipeline_clientes()
            print(f"\n✓ Clientes procesados: {resultados['filas_procesadas']}")
            return

        # Solo tarjetas
        if args.tarjetas:
            logger.info("Ejecutando pipeline de tarjetas...")
            resultados = ejecutar_pipeline_tarjetas()
            print(f"\n✓ Tarjetas procesadas: {resultados['filas_procesadas']}")
            return

        # Solo pipeline (sin BD)
        if args.pipeline:
            logger.info("Ejecutando pipelines (sin carga a BD)...")
            ejecutar_etl_completo(cargar_a_bd=False)
            return

        # Por defecto: ETL completo
        logger.info("Ejecutando ETL completo...")
        resultados = ejecutar_etl_completo(cargar_a_bd=True)
        
        print("\n" + "=" * 50)
        print("RESUMEN DE EJECUCIÓN")
        print("=" * 50)
        print(f"Clientes procesados: {resultados['clientes'].get('filas_procesadas', 0)}")
        print(f"Tarjetas procesadas: {resultados['tarjetas'].get('filas_procesadas', 0)}")
        print(f"Clientes en BD: {resultados['bd'].get('clientes_insertados', 0)}")
        print(f"Tarjetas en BD: {resultados['bd'].get('tarjetas_insertadas', 0)}")
        print("=" * 50)

    except KeyboardInterrupt:
        logger.info("Proceso interrumpido por el usuario")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
