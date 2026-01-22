"""
Ejemplos de uso del sistema de logging
Muestra cómo integrar el logging en diferentes módulos del ETL
"""

from utils.logger import get_logger, get_etl_logger, ETLLogger


def ejemplo_logging_basico():
    """Ejemplo básico de uso del logger"""

    logger = get_logger('ejemplo_basico')
    
    logger.debug("Este es un mensaje de DEBUG - detalles técnicos")
    logger.info("Este es un mensaje de INFO - información general")
    logger.warning("Este es un mensaje de WARNING - advertencia")
    logger.error("Este es un mensaje de ERROR - error recuperable")
    logger.critical("Este es un mensaje de CRITICAL - error crítico")


def ejemplo_logging_etl():
    """Ejemplo de logging para procesos ETL"""
    # Obtener logger específico para ETL
    logger = get_etl_logger('extraccion')
    
    # Inicio del proceso
    logger.info("=" * 80)
    logger.info("Iniciando proceso de extracción de datos")
    logger.info("=" * 80)
    
    try:
        # Simular lectura
        logger.info("Leyendo archivo CSV: datos_clientes.csv")
        registros_leidos = 1500
        logger.info(f"Registros leídos exitosamente: {registros_leidos}")
        
        # Simular validación
        logger.info("Validando datos...")
        registros_validos = 1450
        registros_invalidos = 50
        logger.warning(f"Registros inválidos encontrados: {registros_invalidos}")
        logger.info(f"Registros válidos: {registros_validos}")
        
        # Simular transformación
        logger.info("Aplicando transformaciones...")
        logger.debug("Normalizando nombres...")
        logger.debug("Validando DNI...")
        logger.debug("Anonimizando datos sensibles...")
        
        # Éxito
        logger.info("Proceso completado exitosamente")
        logger.info(f"Total de registros procesados: {registros_validos}")
        
    except Exception as e:
        logger.error(f"Error durante el proceso ETL: {str(e)}", exc_info=True)
        raise


def ejemplo_logging_con_contexto():
    """Ejemplo de logging con información contextual"""
    logger = get_etl_logger('carga_db')
    
    # Información del proceso
    proceso = "Carga de datos a PostgreSQL"
    tabla = "clientes"
    
    logger.info(f"Iniciando {proceso}")
    logger.info(f"Tabla destino: {tabla}")
    
    try:
        # Simular conexión a BD
        logger.debug("Estableciendo conexión con la base de datos...")
        logger.info("Conexión establecida exitosamente")
        
        # Simular inserción
        batch_size = 100
        total_registros = 1450
        
        for i in range(0, total_registros, batch_size):
            registros_insertados = min(batch_size, total_registros - i)
            logger.debug(f"Insertando batch {i//batch_size + 1}: {registros_insertados} registros")
        
        logger.info(f"Inserción completada: {total_registros} registros en tabla '{tabla}'")
        
    except Exception as e:
        logger.error(f"Error al cargar datos en '{tabla}': {str(e)}", exc_info=True)
        raise
    finally:
        logger.debug("Cerrando conexión a base de datos")


def ejemplo_logging_avanzado():
    """Ejemplo de uso avanzado con ETLLogger"""
    etl_logger = ETLLogger()
    logger = etl_logger.create_etl_logger('pipeline_completo')
    
    # Registrar inicio del proceso
    etl_logger.log_etl_process(
        logger=logger,
        process_name="Pipeline ETL Completo",
        status='START'
    )
    
    try:
        # Fase 1: Extracción
        logger.info("FASE 1: Extracción")
        registros = 1500
        etl_logger.log_etl_process(
            logger=logger,
            process_name="Extracción de CSV",
            records_processed=registros,
            status='SUCCESS'
        )
        
        # Fase 2: Transformación
        logger.info("FASE 2: Transformación")
        registros_transformados = 1450
        etl_logger.log_etl_process(
            logger=logger,
            process_name="Transformación y validación",
            records_processed=registros_transformados,
            status='SUCCESS'
        )
        
        # Fase 3: Carga
        logger.info("FASE 3: Carga")
        etl_logger.log_etl_process(
            logger=logger,
            process_name="Carga a base de datos",
            records_processed=registros_transformados,
            status='SUCCESS'
        )
        
        # Proceso completado
        logger.info("=" * 80)
        logger.info("PIPELINE ETL COMPLETADO EXITOSAMENTE")
        logger.info("=" * 80)
        
    except Exception as e:
        etl_logger.log_etl_process(
            logger=logger,
            process_name="Pipeline ETL Completo",
            status='ERROR'
        )
        logger.error(f"Error en el pipeline: {str(e)}", exc_info=True)


if __name__ == "__main__":
    print("Ejecutando ejemplos de logging...\n")
    
    print("1. Logging básico:")
    ejemplo_logging_basico()
    
    print("\n2. Logging ETL:")
    ejemplo_logging_etl()
    
    print("\n3. Logging con contexto:")
    ejemplo_logging_con_contexto()
    
    print("\n4. Logging avanzado:")
    ejemplo_logging_avanzado()
    
    print("\n✓ Ejemplos completados. Revisa la carpeta 'logs/' para ver los archivos generados.")
