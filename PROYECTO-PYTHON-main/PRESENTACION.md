# 🎓 Presentación del Proyecto ETL

## 📋 Información del Proyecto

| Campo | Valor |
|-------|-------|
| **Nombre** | Sistema ETL de Clientes y Tarjetas |
| **Tecnología** | Python 3.8+ |
| **Base de Datos** | PostgreSQL |
| **Tipo** | Pipeline de datos automatizado |

---

## 🎯 Objetivo del Proyecto

Desarrollar un sistema ETL (Extract, Transform, Load) que procese archivos CSV de clientes y tarjetas de crédito, aplicando validaciones y transformaciones, para almacenarlos de forma segura en una base de datos PostgreSQL.

---

## 🏗️ Arquitectura

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   EXTRACCIÓN    │───▶│ TRANSFORMACIÓN  │───▶│     CARGA       │
│   (CSV Files)   │    │  (Validación +  │    │  (PostgreSQL)   │
│                 │    │  Normalización) │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Flujo de Datos

1. **Extracción**: Lee archivos CSV desde `ficheros/`
2. **Transformación**: 
   - Valida DNI, emails, teléfonos, números de tarjeta
   - Normaliza nombres, direcciones, formatos
   - Enmascara datos sensibles (tarjetas)
   - Hashea información confidencial (CVV)
3. **Carga**: Inserta datos limpios en PostgreSQL

---

## 📁 Estructura del Proyecto

```
proyecto/
├── app/                    # Módulo principal
│   ├── __init__.py        # Inicialización
│   ├── config.py          # ⚙️ Configuración centralizada
│   ├── logger.py          # 📝 Sistema de logging
│   ├── database.py        # 🗄️ Conexión PostgreSQL
│   ├── validators.py      # ✅ Validadores de datos
│   ├── normalizers.py     # 🔄 Normalizadores
│   ├── utils.py           # 🛠️ Utilidades
│   ├── pipeline.py        # 🔀 Pipeline ETL
│   └── automation.py      # ⏰ Automatización
├── ficheros/              # 📂 CSVs de entrada
├── logs/                  # 📋 Archivos de log
├── run.py                 # 🚀 Punto de entrada
├── requirements.txt       # 📦 Dependencias
└── .env                   # 🔐 Credenciales (no en git)
```

---

## 🔧 Componentes Principales

### 1. Configuración (`app/config.py`)
- Rutas de archivos y directorios
- Credenciales de base de datos (desde `.env`)
- Patrones de validación (regex)
- Configuración de logging
- Esquemas de tablas SQL

### 2. Validadores (`app/validators.py`)
- `validar_dni()` - Formato español (8 números + letra)
- `validar_email()` - Formato estándar de email
- `validar_telefono()` - 9 dígitos españoles
- `validar_numero_tarjeta()` - 13-19 dígitos
- `validar_cvv()` - 3-4 dígitos
- `validar_fecha_expiracion()` - MM/YY

### 3. Normalizadores (`app/normalizers.py`)
- Normalización de texto (mayúsculas, tildes)
- Limpieza de teléfonos y emails
- Formateo de direcciones
- Diccionarios por tipo de dato

### 4. Pipeline (`app/pipeline.py`)
- `PipelineBase` - Clase abstracta base
- `PipelineClientes` - Procesa clientes
- `PipelineTarjetas` - Procesa tarjetas
- `ETLOrchestrator` - Coordina todo

### 5. Base de Datos (`app/database.py`)
- Patrón Singleton para conexión única
- Creación automática de tablas
- Inserción masiva de registros

---

## 🚀 Demostración

### Paso 1: Configurar entorno
```bash
# Crear entorno virtual
python -m venv venv
venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt

# Configurar credenciales
copy .env.example .env
# Editar .env con datos de PostgreSQL
```

### Paso 2: Preparar base de datos
```bash
python run.py --create-tables
```

### Paso 3: Ejecutar pipeline
```bash
# Pipeline completo
python run.py --pipeline

# Solo clientes
python run.py --clientes

# Solo tarjetas
python run.py --tarjetas
```

### Paso 4: Verificar resultados
```sql
-- En PostgreSQL
SELECT * FROM clientes LIMIT 5;
SELECT * FROM tarjetas LIMIT 5;
```

---

## 📊 Ejemplo de Transformación

### Entrada (CSV)
```csv
dni,nombre,telefono,email
12345678A,  JUAN GARCÍA  ,612345678,JUAN@EMAIL.COM
```

### Salida (Base de Datos)
| dni | nombre | telefono | email |
|-----|--------|----------|-------|
| 12345678A | Juan García | 612345678 | juan@email.com |

### Transformaciones aplicadas:
- ✅ DNI validado (formato correcto)
- 🔄 Nombre normalizado (capitalizado, sin espacios extra)
- 📞 Teléfono limpiado (solo dígitos)
- 📧 Email en minúsculas

---

## 🔐 Seguridad

| Dato | Protección |
|------|------------|
| Número de tarjeta | Enmascarado (`****-****-****-1234`) |
| CVV | Hash SHA-256 con salt |
| Credenciales DB | Variables de entorno (`.env`) |

---

## 📈 Características Destacadas

1. **Modularidad**: Cada componente es independiente y reutilizable
2. **Extensibilidad**: Fácil añadir nuevos validadores/normalizadores
3. **Logging**: Registro completo de operaciones y errores
4. **Automatización**: Ejecución programada con `schedule`
5. **Seguridad**: Protección de datos sensibles
6. **Mantenibilidad**: Código limpio y documentado

---

## 🛠️ Tecnologías Utilizadas

| Tecnología | Uso |
|------------|-----|
| Python 3.8+ | Lenguaje principal |
| PostgreSQL | Base de datos |
| SQLAlchemy | ORM/Conexión DB |
| Pandas | Procesamiento CSV |
| python-dotenv | Variables de entorno |
| schedule | Automatización |

---

## 📝 Comandos Disponibles

```bash
python run.py --help          # Ver ayuda
python run.py --pipeline      # Ejecutar todo
python run.py --clientes      # Solo clientes
python run.py --tarjetas      # Solo tarjetas
python run.py --create-tables # Crear tablas
python run.py --test-db       # Probar conexión
python run.py --schedule      # Modo automático
```

---

## 🎓 Conclusión

Este proyecto demuestra:
- Diseño de arquitectura ETL
- Buenas prácticas de Python
- Manejo seguro de datos sensibles
- Patrones de diseño (Singleton, Template)
- Integración con bases de datos
- Automatización de procesos

---

## ❓ Preguntas Frecuentes

**¿Por qué SQLAlchemy en lugar de psycopg2 directo?**
> SQLAlchemy proporciona una capa de abstracción que facilita el mantenimiento y permite cambiar de base de datos fácilmente.

**¿Por qué se enmascara la tarjeta en lugar de cifrarla?**
> El enmascaramiento es irreversible, cumpliendo con normativas PCI-DSS. Si se necesita el número completo, se debe obtener del sistema original.

**¿Cómo se manejan los errores?**
> El sistema tiene logging completo y los registros inválidos se rechazan sin detener el pipeline.
