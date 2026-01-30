# 📊 ETL Pipeline - Procesamiento de Clientes y Tarjetas

Sistema ETL (Extract, Transform, Load) modular para el procesamiento automatizado de datos de clientes y tarjetas de crédito con almacenamiento en PostgreSQL.

---

## 🚀 Características

- **Pipeline modular**: Procesamiento separado para clientes y tarjetas
- **Validación robusta**: DNI, teléfonos, emails, números de tarjeta
- **Normalización de datos**: Limpieza y estandarización automática
- **Base de datos PostgreSQL**: Almacenamiento persistente con SQLAlchemy
- **Logging completo**: Registro detallado con rotación de archivos
- **Automatización**: Ejecución programada con schedule
- **Integración Dropbox**: Descarga opcional de archivos desde la nube

---

## 📁 Estructura del Proyecto

```
proyecto/
├── app/                    # Módulo principal
│   ├── __init__.py        # Inicialización del paquete
│   ├── config.py          # Configuración centralizada
│   ├── logger.py          # Sistema de logging
│   ├── database.py        # Conexión y operaciones DB
│   ├── validators.py      # Validadores de datos
│   ├── normalizers.py     # Normalizadores de campos
│   ├── utils.py           # Utilidades generales
│   ├── pipeline.py        # Pipelines ETL
│   └── automation.py      # Automatización programada
├── ficheros/              # Archivos CSV de entrada
├── Pipeline/              # Archivos legacy (referencia)
├── run.py                 # Punto de entrada principal
├── requirements.txt       # Dependencias Python
├── .env.example           # Plantilla de configuración
└── README.md              # Este archivo
```

---

## 📋 Requisitos Previos

- Python 3.8 o superior
- PostgreSQL instalado y funcionando
- Git (opcional, para control de versiones)

---

## ⚙️ Instalación

### 1. Clonar el proyecto

```bash
git clone <URL_DEL_REPOSITORIO>
cd proyecto-etl
```

### 2. Crear entorno virtual

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Configurar variables de entorno

```bash
# Copiar plantilla
copy .env.example .env  # Windows
cp .env.example .env    # Linux/Mac

# Editar .env con tus credenciales
```

### 5. Crear tablas en PostgreSQL

```bash
python run.py --create-tables
```

---

## 🎯 Uso

### Ejecutar pipeline completo (clientes + tarjetas)

```bash
python run.py --pipeline
```

### Ejecutar solo clientes

```bash
python run.py --clientes
```

### Ejecutar solo tarjetas

```bash
python run.py --tarjetas
```

### Probar conexión a base de datos

```bash
python run.py --test-db
```

### Ejecutar con automatización programada

```bash
python run.py --schedule
```

---

## 🗄️ Estructura de Datos

### Tabla: clientes

| Campo       | Tipo         | Descripción              |
|-------------|--------------|--------------------------|
| id          | SERIAL       | Identificador único      |
| dni         | VARCHAR(20)  | DNI del cliente          |
| nombre      | VARCHAR(100) | Nombre completo          |
| telefono    | VARCHAR(20)  | Número de teléfono       |
| email       | VARCHAR(100) | Correo electrónico       |
| direccion   | TEXT         | Dirección completa       |
| created_at  | TIMESTAMP    | Fecha de creación        |

### Tabla: tarjetas

| Campo           | Tipo         | Descripción              |
|-----------------|--------------|--------------------------|
| id              | SERIAL       | Identificador único      |
| numero_tarjeta  | VARCHAR(100) | Número enmascarado       |
| cvv_hash        | VARCHAR(100) | CVV hasheado             |
| fecha_expiracion| VARCHAR(10)  | Fecha de expiración      |
| tipo            | VARCHAR(50)  | Tipo de tarjeta          |
| limite_credito  | DECIMAL      | Límite de crédito        |
| cliente_id      | VARCHAR(20)  | DNI del cliente asociado |
| created_at      | TIMESTAMP    | Fecha de creación        |

---

## 📝 Configuración

El archivo `.env` soporta las siguientes variables:

```bash
# Base de datos
DB_HOST=localhost
DB_PORT=5432
DB_NAME=clientes
DB_USER=tu_usuario
DB_PASSWORD=tu_password

# Dropbox (opcional)
DROPBOX_TOKEN=tu_token

# Seguridad
ETL_HASH_SALT=tu_salt_secreto

# Automatización
SCHEDULE_TIME=08:00
```

---

## 🔧 Desarrollo

### Agregar nuevo validador

```python
# En app/validators.py
def validar_nuevo_campo(valor):
    """Valida un nuevo tipo de campo."""
    if not valor:
        return False
    # Tu lógica de validación
    return True
```

### Agregar nuevo normalizador

```python
# En app/normalizers.py
def normalizar_nuevo_campo(valor):
    """Normaliza un nuevo tipo de campo."""
    if pd.isna(valor):
        return None
    # Tu lógica de normalización
    return valor_normalizado
```

---

## 📊 Logs

Los logs se almacenan en `logs/etl.log` con rotación automática:
- Tamaño máximo: 10MB por archivo
- Backups: 5 archivos

---

## 🤝 Contribuir

1. Fork el proyecto
2. Crea tu rama de feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit tus cambios (`git commit -m 'Agrega nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Abre un Pull Request

---

## 📄 Licencia

Este proyecto está bajo la Licencia MIT.
