# 📖 Guía de Uso - Sistema ETL

## Índice
1. [Instalación Rápida](#-instalación-rápida)
2. [Configuración](#-configuración)
3. [Uso Básico](#-uso-básico)
4. [Uso Avanzado](#-uso-avanzado)
5. [Estructura de Archivos CSV](#-estructura-de-archivos-csv)
6. [Solución de Problemas](#-solución-de-problemas)

---

## 🚀 Instalación Rápida

### Windows
```powershell
# 1. Abrir PowerShell en la carpeta del proyecto

# 2. Crear entorno virtual
python -m venv venv

# 3. Activar entorno
.\venv\Scripts\Activate

# 4. Instalar dependencias
pip install -r requirements.txt

# 5. Configurar credenciales
copy .env.example .env
notepad .env  # Editar con tus datos
```

### Linux/Mac
```bash
# 1. Crear entorno virtual
python3 -m venv venv

# 2. Activar entorno
source venv/bin/activate

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar credenciales
cp .env.example .env
nano .env  # Editar con tus datos
```

---

## ⚙️ Configuración

### Archivo `.env`
```env
# Base de datos PostgreSQL
DB_HOST=localhost          # Servidor de PostgreSQL
DB_PORT=5432               # Puerto (por defecto 5432)
DB_NAME=clientes           # Nombre de la base de datos
DB_USER=tu_usuario         # Usuario de PostgreSQL
DB_PASSWORD=tu_password    # Contraseña

# Opcional: Dropbox
DROPBOX_TOKEN=tu_token     # Token de API de Dropbox

# Seguridad
ETL_HASH_SALT=mi_salt_secreto  # Salt para hashear CVV

# Automatización
SCHEDULE_TIME=08:00        # Hora de ejecución automática
```

### Crear Base de Datos en PostgreSQL
```sql
-- Conectar a PostgreSQL como superusuario
CREATE DATABASE clientes;
CREATE USER tu_usuario WITH PASSWORD 'tu_password';
GRANT ALL PRIVILEGES ON DATABASE clientes TO tu_usuario;
```

---

## 🎯 Uso Básico

### Ver ayuda
```bash
python run.py --help
```

### Crear tablas en la base de datos
```bash
python run.py --create-tables
```
Esto crea las tablas `clientes` y `tarjetas` si no existen.

### Probar conexión a la base de datos
```bash
python run.py --test-db
```
Verifica que las credenciales sean correctas.

### Ejecutar pipeline completo
```bash
python run.py --pipeline
```
Procesa todos los archivos CSV de clientes y tarjetas.

### Ejecutar solo clientes
```bash
python run.py --clientes
```

### Ejecutar solo tarjetas
```bash
python run.py --tarjetas
```

---

## 🔧 Uso Avanzado

### Modo Automático (Scheduler)
```bash
python run.py --schedule
```
Ejecuta el pipeline automáticamente a la hora configurada en `SCHEDULE_TIME`.

### Usar como Módulo Python
```python
from app.pipeline import ETLOrchestrator
from app.database import Database

# Inicializar
db = Database()
orchestrator = ETLOrchestrator(db)

# Ejecutar
orchestrator.ejecutar_pipeline_completo()
```

### Procesar un archivo específico
```python
from app.pipeline import PipelineClientes
from app.database import Database

db = Database()
pipeline = PipelineClientes(db)

# Procesar archivo específico
pipeline.procesar("ficheros/mi_archivo.csv")
```

### Usar validadores individualmente
```python
from app.validators import validar_dni, validar_email

# Validar datos
if validar_dni("12345678A"):
    print("DNI válido")

if validar_email("usuario@email.com"):
    print("Email válido")
```

### Usar normalizadores
```python
from app.normalizers import normalizar_nombre, normalizar_email

nombre = normalizar_nombre("  JUAN GARCÍA  ")  # "Juan García"
email = normalizar_email("USUARIO@EMAIL.COM")   # "usuario@email.com"
```

---

## 📂 Estructura de Archivos CSV

### Clientes (`Clientes-*.csv`)
```csv
dni,nombre,telefono,email,direccion
12345678A,Juan García,612345678,juan@email.com,Calle Mayor 1
```

| Campo | Tipo | Validación |
|-------|------|------------|
| dni | String | 8 dígitos + 1 letra |
| nombre | String | Texto |
| telefono | String | 9 dígitos |
| email | String | Formato email |
| direccion | String | Texto libre |

### Tarjetas (`Tarjetas-*.csv`)
```csv
numero_tarjeta,cvv,fecha_expiracion,tipo,limite_credito,cliente_id
4111111111111111,123,12/25,VISA,5000.00,12345678A
```

| Campo | Tipo | Validación |
|-------|------|------------|
| numero_tarjeta | String | 13-19 dígitos |
| cvv | String | 3-4 dígitos |
| fecha_expiracion | String | MM/YY |
| tipo | String | VISA/MASTERCARD/etc |
| limite_credito | Decimal | Número positivo |
| cliente_id | String | DNI válido |

---

## 🔍 Logs

Los logs se guardan en `logs/etl.log`:

```
2026-01-30 10:00:00 - INFO - Iniciando pipeline de clientes
2026-01-30 10:00:01 - INFO - Procesando ficheros/Clientes-2026-01-19.csv
2026-01-30 10:00:02 - INFO - 150 registros procesados, 145 válidos, 5 rechazados
2026-01-30 10:00:03 - INFO - Pipeline completado exitosamente
```

---

## ❌ Solución de Problemas

### Error: "No module named 'app'"
```bash
# Asegúrate de estar en la carpeta correcta
cd PROYECTO-PYTHON-main

# Verifica el entorno virtual
.\venv\Scripts\Activate  # Windows
source venv/bin/activate  # Linux/Mac
```

### Error: "Connection refused" (PostgreSQL)
1. Verifica que PostgreSQL esté ejecutándose
2. Comprueba las credenciales en `.env`
3. Asegúrate de que la base de datos existe:
```sql
CREATE DATABASE clientes;
```

### Error: "Permission denied"
```sql
-- Dar permisos al usuario
GRANT ALL PRIVILEGES ON DATABASE clientes TO tu_usuario;
GRANT ALL ON ALL TABLES IN SCHEMA public TO tu_usuario;
```

### Error: "Invalid DNI format"
El DNI debe tener formato español: 8 números + 1 letra mayúscula.
- ✅ Correcto: `12345678A`
- ❌ Incorrecto: `1234567A`, `12345678a`, `A12345678`

### No se procesan archivos
Verifica que los archivos CSV estén en `ficheros/` con el nombre correcto:
- `Clientes-YYYY-MM-DD.csv`
- `Tarjetas-YYYY-MM-DD.csv`

---

## 📞 Soporte

Si tienes problemas:
1. Revisa los logs en `logs/etl.log`
2. Ejecuta `python run.py --test-db` para verificar la conexión
3. Asegúrate de que el entorno virtual esté activo
