# 📋 Sistema de Logging - Documentación

**Autor:** Cristian  
**Semana:** 4 - Integración y Carga de Datos  
**Tarea:** Sistema de logging implementado

---

## 📖 Descripción General

Este sistema de logging proporciona una solución centralizada y robusta para registrar eventos, errores y procesos del pipeline ETL. Incluye:

- ✅ Logging a archivo y consola
- ✅ Rotación automática de archivos (por tamaño o tiempo)
- ✅ Diferentes niveles de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- ✅ Formato estructurado con timestamps y contexto
- ✅ Funciones específicas para procesos ETL
- ✅ Patrón Singleton para evitar duplicación

---

## 🚀 Inicio Rápido

### Uso Básico

```python
from utils.logger import get_logger

# Crear un logger
logger = get_logger('mi_modulo')

# Registrar mensajes
logger.info("Proceso iniciado")
logger.warning("Advertencia: datos incompletos")
logger.error("Error al procesar archivo")
```

### Uso para ETL

```python
from utils.logger import get_etl_logger

# Crear logger específico para ETL
logger = get_etl_logger('extraccion')

logger.info("Iniciando extracción de datos")
logger.info(f"Registros procesados: {count}")
logger.info("Extracción completada")
```

---

## 📁 Estructura de Archivos

```
Proyecto Python/
├── logs/                          # Directorio de logs (se crea automáticamente)
│   ├── etl_extraccion.log        # Logs de extracción
│   ├── etl_transformacion.log    # Logs de transformación
│   ├── etl_carga.log             # Logs de carga
│   └── etl_pipeline_completo.log # Logs del pipeline completo
├── src/
│   └── utils/
│       ├── __init__.py           # Exporta funciones de logging
│       ├── logger.py             # Sistema de logging principal
│       └── logging_examples.py   # Ejemplos de uso
```

---

## 🎯 Niveles de Log

| Nivel    | Uso                                          | Ejemplo                                    |
|----------|----------------------------------------------|--------------------------------------------|
| DEBUG    | Información detallada para debugging         | `logger.debug("Variable x = 10")`          |
| INFO     | Información general del proceso              | `logger.info("Proceso completado")`        |
| WARNING  | Advertencias que no detienen el proceso      | `logger.warning("Dato faltante")`          |
| ERROR    | Errores que pueden ser manejados             | `logger.error("Error al leer archivo")`    |
| CRITICAL | Errores críticos que detienen el sistema     | `logger.critical("BD no disponible")`      |

---

## 🔧 Funciones Principales

### 1. `get_logger(name, level='INFO')`

Obtiene un logger genérico configurado.

**Parámetros:**
- `name` (str): Nombre del logger (generalmente `__name__`)
- `level` (str): Nivel de logging (default: 'INFO')

**Ejemplo:**
```python
from utils.logger import get_logger

logger = get_logger(__name__, level='DEBUG')
logger.debug("Mensaje de debug")
```

### 2. `get_etl_logger(module_name)`

Crea un logger específico para módulos ETL con rotación diaria.

**Parámetros:**
- `module_name` (str): Nombre del módulo ETL

**Ejemplo:**
```python
from utils.logger import get_etl_logger

logger = get_etl_logger('transformacion')
logger.info("Transformando datos...")
```

### 3. `ETLLogger.log_etl_process(logger, process_name, records_processed, status)`

Registra información estructurada sobre un proceso ETL.

**Parámetros:**
- `logger`: Logger a utilizar
- `process_name` (str): Nombre del proceso
- `records_processed` (int): Número de registros procesados
- `status` (str): Estado del proceso ('START', 'PROGRESS', 'SUCCESS', 'ERROR')

**Ejemplo:**
```python
from utils.logger import get_etl_logger, ETLLogger

logger = get_etl_logger('carga')
etl_logger = ETLLogger()

etl_logger.log_etl_process(
    logger=logger,
    process_name="Carga a PostgreSQL",
    records_processed=1500,
    status='SUCCESS'
)
```

---

## 💡 Ejemplos de Integración

### Ejemplo 1: Módulo de Extracción CSV

```python
from utils.logger import get_etl_logger
import pandas as pd

logger = get_etl_logger('extraccion')

def extraer_csv(filepath):
    logger.info(f"Iniciando lectura de archivo: {filepath}")
    
    try:
        df = pd.read_csv(filepath)
        logger.info(f"Archivo leído exitosamente: {len(df)} registros")
        return df
    except FileNotFoundError:
        logger.error(f"Archivo no encontrado: {filepath}")
        raise
    except Exception as e:
        logger.error(f"Error al leer CSV: {str(e)}", exc_info=True)
        raise
```

### Ejemplo 2: Módulo de Transformación

```python
from utils.logger import get_etl_logger

logger = get_etl_logger('transformacion')

def validar_dni(dni):
    logger.debug(f"Validando DNI: {dni}")
    
    if not dni or len(dni) != 9:
        logger.warning(f"DNI inválido: {dni}")
        return False
    
    logger.debug(f"DNI válido: {dni}")
    return True

def transformar_datos(df):
    logger.info("Iniciando transformación de datos")
    logger.info(f"Registros a transformar: {len(df)}")
    
    # Validaciones
    validos = df['dni'].apply(validar_dni).sum()
    invalidos = len(df) - validos
    
    logger.info(f"Registros válidos: {validos}")
    logger.warning(f"Registros inválidos: {invalidos}")
    
    return df
```

### Ejemplo 3: Módulo de Carga a Base de Datos

```python
from utils.logger import get_etl_logger, ETLLogger
import psycopg2

logger = get_etl_logger('carga')
etl_logger = ETLLogger()

def cargar_a_db(df, tabla):
    etl_logger.log_etl_process(
        logger=logger,
        process_name=f"Carga a tabla {tabla}",
        status='START'
    )
    
    try:
        logger.info("Conectando a base de datos...")
        conn = psycopg2.connect(...)
        
        logger.info(f"Insertando {len(df)} registros en tabla '{tabla}'")
        # Lógica de inserción...
        
        etl_logger.log_etl_process(
            logger=logger,
            process_name=f"Carga a tabla {tabla}",
            records_processed=len(df),
            status='SUCCESS'
        )
        
    except Exception as e:
        etl_logger.log_etl_process(
            logger=logger,
            process_name=f"Carga a tabla {tabla}",
            status='ERROR'
        )
        logger.error(f"Error en carga: {str(e)}", exc_info=True)
        raise
```

### Ejemplo 4: Pipeline ETL Completo

```python
from utils.logger import get_etl_logger, ETLLogger

logger = get_etl_logger('pipeline')
etl_logger = ETLLogger()

def ejecutar_pipeline():
    logger.info("=" * 80)
    logger.info("INICIANDO PIPELINE ETL COMPLETO")
    logger.info("=" * 80)
    
    try:
        # Extracción
        logger.info("FASE 1: Extracción")
        df = extraer_csv('datos.csv')
        etl_logger.log_etl_process(logger, "Extracción", len(df), 'SUCCESS')
        
        # Transformación
        logger.info("FASE 2: Transformación")
        df_transformado = transformar_datos(df)
        etl_logger.log_etl_process(logger, "Transformación", len(df_transformado), 'SUCCESS')
        
        # Carga
        logger.info("FASE 3: Carga")
        cargar_a_db(df_transformado, 'clientes')
        
        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETADO EXITOSAMENTE")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.critical(f"Pipeline fallido: {str(e)}", exc_info=True)
        raise
```

---

## 🔄 Rotación de Archivos

### Rotación por Tamaño
- Tamaño máximo: **10 MB** por archivo
- Backups mantenidos: **5 archivos**
- Formato: `archivo.log`, `archivo.log.1`, `archivo.log.2`, etc.

### Rotación por Tiempo (ETL)
- Frecuencia: **Diaria** (medianoche)
- Backups mantenidos: **30 días**
- Formato: `archivo.log.YYYY-MM-DD`

---

## 📊 Formato de Log

Cada línea de log incluye:

```
2026-01-22 16:45:30 | etl.extraccion | INFO     | extract.py:45 | extraer_csv() | Archivo leído: 1500 registros
```

**Componentes:**
1. **Timestamp**: Fecha y hora del evento
2. **Logger**: Nombre del logger
3. **Nivel**: Nivel de severidad
4. **Archivo**: Archivo fuente y línea
5. **Función**: Función que generó el log
6. **Mensaje**: Descripción del evento

---

## 🧪 Probar el Sistema

Ejecuta el archivo de ejemplos:

```bash
cd "C:\Users\cristian.barquero\OneDrive - Fundación Universitaria San Pablo CEU\Escritorio\Proyecto Python"
python -m src.utils.logging_examples
```

Esto generará:
- Logs en consola con colores
- Archivos de log en la carpeta `logs/`

---

## ✅ Checklist de Integración

Para integrar el logging en tu módulo ETL:

- [ ] Importar el logger: `from utils.logger import get_etl_logger`
- [ ] Crear logger al inicio del módulo: `logger = get_etl_logger('nombre_modulo')`
- [ ] Agregar log de inicio: `logger.info("Iniciando proceso...")`
- [ ] Agregar logs de progreso: `logger.info(f"Procesados: {count}")`
- [ ] Agregar logs de error con try/except: `logger.error("Error", exc_info=True)`
- [ ] Agregar log de finalización: `logger.info("Proceso completado")`

---

## 🎓 Buenas Prácticas

1. **Usa el nivel apropiado:**
   - `DEBUG` para detalles técnicos
   - `INFO` para flujo normal del programa
   - `WARNING` para situaciones anormales pero manejables
   - `ERROR` para errores que requieren atención
   - `CRITICAL` para fallos del sistema

2. **Incluye contexto:**
   ```python
   # ❌ Malo
   logger.error("Error")
   
   # ✅ Bueno
   logger.error(f"Error al procesar archivo {filename}: {str(e)}", exc_info=True)
   ```

3. **Usa exc_info=True para excepciones:**
   ```python
   try:
       # código
   except Exception as e:
       logger.error(f"Error: {str(e)}", exc_info=True)  # Incluye stack trace
   ```

4. **Evita logging excesivo en loops:**
   ```python
   # ❌ Malo (1000 logs)
   for item in items:
       logger.info(f"Procesando {item}")
   
   # ✅ Bueno
   logger.info(f"Procesando {len(items)} items...")
   for i, item in enumerate(items):
       if i % 100 == 0:
           logger.debug(f"Progreso: {i}/{len(items)}")
   ```

---

## 🔍 Troubleshooting

### Los logs no se generan
- Verifica que el directorio `logs/` tenga permisos de escritura
- Comprueba el nivel de logging (debe ser <= al nivel del mensaje)

### Logs duplicados
- El sistema usa `propagate=False` para evitar duplicación
- Si persiste, verifica que no estés creando múltiples loggers con el mismo nombre

### Archivos de log muy grandes
- Ajusta `maxBytes` en `_create_file_handler()` para rotación más frecuente
- Reduce el `backupCount` para mantener menos archivos

---

## 📞 Soporte

Para dudas o problemas con el sistema de logging:
- Revisa los ejemplos en `src/utils/logging_examples.py`
- Consulta esta documentación
- Contacta a Cristian (responsable del sistema de logging)

---

**Última actualización:** Semana 4 - Enero 2026
