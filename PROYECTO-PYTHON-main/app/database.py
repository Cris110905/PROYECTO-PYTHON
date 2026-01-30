"""
Módulo de Base de Datos
=======================

Gestiona todas las operaciones de base de datos:
- Conexión a PostgreSQL
- Creación de tablas
- Inserción de datos
- Consultas
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager

try:
    from app import config
    from app.logger import get_logger
except ImportError:
    import config
    from logger import get_logger

logger = get_logger("database")


class Database:
    """
    Clase para gestionar la conexión y operaciones de base de datos.
    """

    _instance = None
    _engine = None

    def __new__(cls):
        """Implementa patrón Singleton"""
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Inicializa la conexión a la base de datos"""
        if self._initialized:
            return

        self._initialized = True
        self._engine = None

    def get_engine(self):
        """
        Obtiene o crea el engine de SQLAlchemy.

        Returns:
            Engine: Motor de SQLAlchemy
        """
        if self._engine is None:
            try:
                self._engine = create_engine(
                    config.DATABASE_URL,
                    pool_pre_ping=True,
                    pool_recycle=3600,
                )
                logger.info("Conexión a base de datos establecida correctamente")
            except Exception as e:
                logger.error(f"Error al conectar a la base de datos: {e}")
                raise

        return self._engine

    @contextmanager
    def get_connection(self):
        """
        Context manager para obtener una conexión.

        Yields:
            Connection: Conexión a la base de datos
        """
        engine = self.get_engine()
        conn = engine.connect()
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Error en transacción: {e}")
            raise
        finally:
            conn.close()

    def create_tables(self):
        """
        Crea las tablas necesarias en la base de datos.
        """
        logger.info("Creando tablas en la base de datos...")

        create_clients_sql = """
            CREATE TABLE IF NOT EXISTS clients (
                cod_cliente VARCHAR(20) PRIMARY KEY,
                nombre VARCHAR(100),
                apellido1 VARCHAR(100),
                apellido2 VARCHAR(100),
                dni VARCHAR(20) UNIQUE,
                dni_hash VARCHAR(64),
                correo VARCHAR(150),
                telefono VARCHAR(20),
                fecha_procesado TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """

        create_tarjetas_sql = """
            CREATE TABLE IF NOT EXISTS tarjetas (
                id SERIAL PRIMARY KEY,
                cod_cliente VARCHAR(20) REFERENCES clients(cod_cliente),
                numero_tarjeta_hash VARCHAR(64),
                numero_tarjeta_masked VARCHAR(25),
                fecha_exp VARCHAR(10),
                cvv_hash VARCHAR(64),
                fecha_procesado TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """

        try:
            with self.get_connection() as conn:
                conn.execute(text(create_clients_sql))
                conn.execute(text(create_tarjetas_sql))
                logger.info("✓ Tablas creadas correctamente")
        except SQLAlchemyError as e:
            logger.error(f"Error al crear tablas: {e}")
            raise

    def insert_clients(self, df):
        """
        Inserta registros de clientes en la base de datos.

        Args:
            df: DataFrame con los datos de clientes

        Returns:
            int: Número de registros insertados
        """
        if df.empty:
            logger.warning("DataFrame de clientes vacío, no hay datos para insertar")
            return 0

        try:
            engine = self.get_engine()
            with engine.begin() as conn:
                df.to_sql("clients", con=conn, if_exists="append", index=False)

            count = len(df)
            logger.info(f"✓ {count} registros de clientes insertados correctamente")
            return count

        except SQLAlchemyError as e:
            logger.error(f"Error al insertar clientes: {e}")
            raise

    def insert_tarjetas(self, df):
        """
        Inserta registros de tarjetas en la base de datos.

        Args:
            df: DataFrame con los datos de tarjetas

        Returns:
            int: Número de registros insertados
        """
        if df.empty:
            logger.warning("DataFrame de tarjetas vacío, no hay datos para insertar")
            return 0

        try:
            engine = self.get_engine()
            with engine.begin() as conn:
                df.to_sql("tarjetas", con=conn, if_exists="append", index=False)

            count = len(df)
            logger.info(f"✓ {count} registros de tarjetas insertados correctamente")
            return count

        except SQLAlchemyError as e:
            logger.error(f"Error al insertar tarjetas: {e}")
            raise

    def get_existing_clients(self):
        """
        Obtiene la lista de códigos de cliente existentes.

        Returns:
            set: Conjunto de códigos de cliente
        """
        try:
            with self.get_connection() as conn:
                result = conn.execute(text("SELECT cod_cliente FROM clients"))
                return set(row[0] for row in result)
        except SQLAlchemyError as e:
            logger.error(f"Error al obtener clientes existentes: {e}")
            return set()

    def test_connection(self):
        """
        Prueba la conexión a la base de datos.

        Returns:
            bool: True si la conexión es exitosa
        """
        try:
            with self.get_connection() as conn:
                conn.execute(text("SELECT 1"))
                logger.info("✓ Conexión a base de datos verificada")
                return True
        except Exception as e:
            logger.error(f"✗ Error de conexión: {e}")
            return False


# =============================================================================
# FUNCIONES HELPER
# =============================================================================


def get_engine():
    """
    Función helper para obtener el engine de SQLAlchemy.

    Returns:
        Engine: Motor de SQLAlchemy
    """
    db = Database()
    return db.get_engine()


def get_database():
    """
    Función helper para obtener la instancia de Database.

    Returns:
        Database: Instancia de la clase Database
    """
    return Database()
