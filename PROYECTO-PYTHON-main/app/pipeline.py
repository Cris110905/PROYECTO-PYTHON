"""
Pipeline ETL Principal
======================

Implementa el pipeline completo de ETL para:
- Clientes: Extracción, normalización, validación y carga
- Tarjetas: Extracción, normalización, anonimización y carga
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, List
from abc import ABC, abstractmethod

try:
    from app import config
    from app.logger import get_logger
    from app.database import get_database
    from app.validators import (
        validar_dni,
        validar_telefono,
        validar_correo,
        validar_fecha_expiracion,
        validar_nombre,
    )
    from app.normalizers import (
        CLIENTES_NORMALIZERS,
        TARJETAS_NORMALIZERS,
        normalizar_columnas_dataframe,
        limpiar_espacios_dataframe,
        eliminar_acentos,
    )
    from app.utils import (
        hash_con_salt,
        enmascarar_tarjeta,
        extraer_fecha_archivo,
        leer_csv_con_encoding,
        detectar_archivos,
        guardar_csv,
        validar_campos_obligatorios,
    )
except ImportError:
    import config
    from logger import get_logger
    from database import get_database
    from validators import (
        validar_dni,
        validar_telefono,
        validar_correo,
        validar_fecha_expiracion,
        validar_nombre,
    )
    from normalizers import (
        CLIENTES_NORMALIZERS,
        TARJETAS_NORMALIZERS,
        normalizar_columnas_dataframe,
        limpiar_espacios_dataframe,
        eliminar_acentos,
    )
    from utils import (
        hash_con_salt,
        enmascarar_tarjeta,
        extraer_fecha_archivo,
        leer_csv_con_encoding,
        detectar_archivos,
        guardar_csv,
        validar_campos_obligatorios,
    )


class PipelineBase(ABC):
    """
    Clase base abstracta para pipelines ETL.
    Define la estructura común de todos los pipelines.
    """

    def __init__(self, nombre: str, pattern: str, campos_obligatorios: List[str]):
        """
        Inicializa el pipeline.

        Args:
            nombre: Nombre del pipeline (para logging)
            pattern: Patrón regex para detectar archivos
            campos_obligatorios: Lista de campos requeridos
        """
        self.nombre = nombre
        self.pattern = pattern
        self.campos_obligatorios = campos_obligatorios
        self.logger = get_logger(f"pipeline.{nombre}")
        self.stats = {
            "filas_leidas": 0,
            "filas_procesadas": 0,
            "filas_rechazadas": 0,
            "archivos_procesados": 0,
        }

    def ejecutar(self, input_dir: Optional[Path] = None) -> dict:
        """
        Ejecuta el pipeline completo.

        Args:
            input_dir: Directorio de entrada (por defecto usa config)

        Returns:
            dict: Estadísticas de la ejecución
        """
        self.logger.info("=" * 80)
        self.logger.info(f"INICIANDO PIPELINE: {self.nombre.upper()}")
        self.logger.info("=" * 80)

        inicio = datetime.now()

        # Buscar archivos en múltiples ubicaciones
        if input_dir is None:
            directorios = [config.INPUT_DIR, config.FICHEROS_DIR, config.DATA_DIR]
        else:
            directorios = [input_dir]

        archivos = []
        for directorio in directorios:
            if directorio.exists():
                archivos.extend(detectar_archivos(directorio, self.pattern))

        if not archivos:
            self.logger.warning("No hay archivos para procesar")
            return self.stats

        for archivo in archivos:
            self._procesar_archivo(archivo)

        fin = datetime.now()
        duracion = (fin - inicio).total_seconds()

        self._mostrar_resumen(duracion)
        return self.stats

    def _procesar_archivo(self, archivo: Path):
        """
        Procesa un archivo individual.

        Args:
            archivo: Ruta al archivo a procesar
        """
        self.logger.info("-" * 60)
        self.logger.info(f"Procesando: {archivo.name}")
        self.logger.info("-" * 60)

        # Leer
        df = leer_csv_con_encoding(archivo)
        if df is None or df.empty:
            self.logger.warning(f"No se pudo leer o está vacío: {archivo.name}")
            return

        self.stats["filas_leidas"] += len(df)

        # Limpiar y normalizar columnas
        df = limpiar_espacios_dataframe(df)
        df = normalizar_columnas_dataframe(df)

        # Normalizar datos específicos
        df = self._normalizar_datos(df)

        # Validar datos
        df = self._validar_datos(df)

        # Separar válidos de rechazados
        df_valido, df_rechazado = validar_campos_obligatorios(df, self.campos_obligatorios)
        self.stats["filas_rechazadas"] += len(df_rechazado)

        # Anonimizar datos sensibles
        df_valido = self._anonimizar_datos(df_valido)

        # Guardar resultados
        self._guardar_resultados(df_valido, df_rechazado, archivo)

        self.stats["filas_procesadas"] += len(df_valido)
        self.stats["archivos_procesados"] += 1

    @abstractmethod
    def _normalizar_datos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza los datos específicos del tipo de archivo."""
        pass

    @abstractmethod
    def _validar_datos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida los datos y añade columnas de validación."""
        pass

    @abstractmethod
    def _anonimizar_datos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Anonimiza datos sensibles."""
        pass

    @abstractmethod
    def _guardar_resultados(
        self, df_valido: pd.DataFrame, df_rechazado: pd.DataFrame, archivo_original: Path
    ):
        """Guarda los resultados procesados."""
        pass

    def _mostrar_resumen(self, duracion: float):
        """
        Muestra el resumen de la ejecución.

        Args:
            duracion: Tiempo de ejecución en segundos
        """
        self.logger.info("=" * 80)
        self.logger.info(f"PIPELINE {self.nombre.upper()} COMPLETADO")
        self.logger.info("=" * 80)
        self.logger.info(f"Archivos procesados: {self.stats['archivos_procesados']}")
        self.logger.info(f"Filas leídas: {self.stats['filas_leidas']}")
        self.logger.info(f"Filas procesadas: {self.stats['filas_procesadas']}")
        self.logger.info(f"Filas rechazadas: {self.stats['filas_rechazadas']}")
        self.logger.info(f"Tiempo: {duracion:.2f} segundos")
        self.logger.info("=" * 80)


class PipelineClientes(PipelineBase):
    """
    Pipeline para procesar archivos de clientes.
    """

    def __init__(self):
        super().__init__(
            nombre="clientes",
            pattern=config.CLIENTES_PATTERN,
            campos_obligatorios=config.REQUIRED_FIELDS["clientes"],
        )

    def _normalizar_datos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza datos de clientes."""
        self.logger.info("Normalizando datos de clientes...")

        for columna, normalizador in CLIENTES_NORMALIZERS.items():
            if columna in df.columns:
                df[columna] = df[columna].apply(
                    lambda x: normalizador(x) if pd.notna(x) and x != "" else x
                )
                self.logger.info(f"  ✓ {columna} normalizado")

        return df

    def _validar_datos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida datos de clientes."""
        self.logger.info("Validando datos de clientes...")

        if "dni" in df.columns:
            df["dni_valido"] = df["dni"].apply(lambda x: validar_dni(x) if x else False)
            validos = df["dni_valido"].sum()
            self.logger.info(f"  ✓ DNI: {validos}/{len(df)} válidos")

        if "telefono" in df.columns:
            df["telefono_valido"] = df["telefono"].apply(
                lambda x: validar_telefono(x) if x else False
            )
            validos = df["telefono_valido"].sum()
            self.logger.info(f"  ✓ Teléfono: {validos}/{len(df)} válidos")

        if "correo" in df.columns:
            df["correo_valido"] = df["correo"].apply(
                lambda x: validar_correo(x) if x else False
            )
            validos = df["correo_valido"].sum()
            self.logger.info(f"  ✓ Correo: {validos}/{len(df)} válidos")

        return df

    def _anonimizar_datos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Anonimiza datos sensibles de clientes."""
        self.logger.info("Anonimizando datos sensibles...")

        if "dni" in df.columns:
            df["dni_hash"] = df["dni"].apply(hash_con_salt)
            self.logger.info("  ✓ DNI hasheado")

        return df

    def _guardar_resultados(
        self, df_valido: pd.DataFrame, df_rechazado: pd.DataFrame, archivo_original: Path
    ):
        """Guarda los resultados de clientes."""
        fecha = extraer_fecha_archivo(archivo_original.name) or datetime.now().strftime(
            "%Y-%m-%d"
        )

        # Columnas a excluir del CSV final
        cols_excluir = ["dni_valido", "telefono_valido", "correo_valido", "motivo_rechazo"]

        if not df_valido.empty:
            cols_exportar = [c for c in df_valido.columns if c not in cols_excluir]
            archivo_salida = config.OUTPUT_DIR / f"Clientes-{fecha}.cleaned.csv"
            guardar_csv(df_valido[cols_exportar], archivo_salida)

        if not df_rechazado.empty:
            archivo_errores = config.ERRORS_DIR / f"Clientes-{fecha}.rejected.csv"
            guardar_csv(df_rechazado, archivo_errores)


class PipelineTarjetas(PipelineBase):
    """
    Pipeline para procesar archivos de tarjetas.
    """

    def __init__(self):
        super().__init__(
            nombre="tarjetas",
            pattern=config.TARJETAS_PATTERN,
            campos_obligatorios=config.REQUIRED_FIELDS["tarjetas"],
        )

    def _normalizar_datos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza datos de tarjetas."""
        self.logger.info("Normalizando datos de tarjetas...")

        for columna, normalizador in TARJETAS_NORMALIZERS.items():
            if columna in df.columns:
                df[f"{columna}_limpio"] = df[columna].apply(
                    lambda x: normalizador(x) if pd.notna(x) and x != "" else x
                )
                self.logger.info(f"  ✓ {columna} normalizado")

        return df

    def _validar_datos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Valida datos de tarjetas."""
        self.logger.info("Validando datos de tarjetas...")

        if "fecha_exp" in df.columns:
            df["fecha_exp_valida"] = df["fecha_exp"].apply(
                lambda x: validar_fecha_expiracion(x) if x else False
            )
            validos = df["fecha_exp_valida"].sum()
            self.logger.info(f"  ✓ Fecha expiración: {validos}/{len(df)} válidos")

        return df

    def _anonimizar_datos(self, df: pd.DataFrame) -> pd.DataFrame:
        """Anonimiza datos sensibles de tarjetas (OBLIGATORIO)."""
        self.logger.info("Anonimizando datos de tarjetas (OBLIGATORIO)...")

        col_tarjeta = "numero_tarjeta_limpio" if "numero_tarjeta_limpio" in df.columns else "numero_tarjeta"
        col_cvv = "cvv_limpio" if "cvv_limpio" in df.columns else "cvv"

        if col_tarjeta in df.columns:
            df["numero_tarjeta_masked"] = df[col_tarjeta].apply(enmascarar_tarjeta)
            df["numero_tarjeta_hash"] = df[col_tarjeta].apply(hash_con_salt)
            self.logger.info("  ✓ Número de tarjeta enmascarado y hasheado")

        if col_cvv in df.columns:
            df["cvv_hash"] = df[col_cvv].apply(hash_con_salt)
            self.logger.info("  ✓ CVV hasheado")

        return df

    def _guardar_resultados(
        self, df_valido: pd.DataFrame, df_rechazado: pd.DataFrame, archivo_original: Path
    ):
        """Guarda los resultados de tarjetas."""
        fecha = extraer_fecha_archivo(archivo_original.name) or datetime.now().strftime(
            "%Y-%m-%d"
        )

        # Columnas sensibles a excluir SIEMPRE
        cols_sensibles = [
            "numero_tarjeta",
            "numero_tarjeta_limpio",
            "cvv",
            "cvv_limpio",
        ]
        cols_internas = ["fecha_exp_valida", "motivo_rechazo"]

        if not df_valido.empty:
            cols_exportar = [
                c for c in df_valido.columns if c not in cols_sensibles + cols_internas
            ]
            archivo_salida = config.OUTPUT_DIR / f"Tarjetas-{fecha}.cleaned.csv"
            guardar_csv(df_valido[cols_exportar], archivo_salida)

        if not df_rechazado.empty:
            cols_exportar_rechazadas = [
                c for c in df_rechazado.columns if c not in cols_sensibles
            ]
            archivo_errores = config.ERRORS_DIR / f"Tarjetas-{fecha}.rejected.csv"
            guardar_csv(df_rechazado[cols_exportar_rechazadas], archivo_errores)


class ETLOrchestrator:
    """
    Orquestador principal del proceso ETL.
    Coordina la ejecución de todos los pipelines y la carga a base de datos.
    """

    def __init__(self):
        self.logger = get_logger("etl.orchestrator")
        self.db = get_database()

    def ejecutar_completo(self, cargar_a_bd: bool = True) -> dict:
        """
        Ejecuta el proceso ETL completo.

        Args:
            cargar_a_bd: Si True, carga los datos procesados a la base de datos

        Returns:
            dict: Estadísticas de la ejecución
        """
        self.logger.info("=" * 80)
        self.logger.info("INICIANDO PROCESO ETL COMPLETO")
        self.logger.info("=" * 80)

        inicio = datetime.now()
        resultados = {"clientes": {}, "tarjetas": {}, "bd": {}}

        # Pipeline de Clientes
        self.logger.info("\n[1/3] Procesando Clientes...")
        pipeline_clientes = PipelineClientes()
        resultados["clientes"] = pipeline_clientes.ejecutar()

        # Pipeline de Tarjetas
        self.logger.info("\n[2/3] Procesando Tarjetas...")
        pipeline_tarjetas = PipelineTarjetas()
        resultados["tarjetas"] = pipeline_tarjetas.ejecutar()

        # Carga a Base de Datos
        if cargar_a_bd:
            self.logger.info("\n[3/3] Cargando a Base de Datos...")
            resultados["bd"] = self._cargar_a_base_datos()

        fin = datetime.now()
        duracion = (fin - inicio).total_seconds()

        self.logger.info("\n" + "=" * 80)
        self.logger.info("PROCESO ETL COMPLETADO")
        self.logger.info(f"Tiempo total: {duracion:.2f} segundos")
        self.logger.info("=" * 80)

        return resultados

    def _cargar_a_base_datos(self) -> dict:
        """
        Carga los datos procesados a la base de datos.

        Returns:
            dict: Estadísticas de la carga
        """
        stats = {"clientes_insertados": 0, "tarjetas_insertadas": 0, "errores": []}

        try:
            # Verificar conexión
            if not self.db.test_connection():
                stats["errores"].append("No se pudo conectar a la base de datos")
                return stats

            # Crear tablas si no existen
            self.db.create_tables()

            # Buscar archivos procesados
            archivos_clientes = list(config.OUTPUT_DIR.glob("Clientes-*.cleaned.csv"))
            archivos_tarjetas = list(config.OUTPUT_DIR.glob("Tarjetas-*.cleaned.csv"))

            # Cargar clientes
            for archivo in archivos_clientes:
                df = pd.read_csv(
                    archivo, sep=config.CSV_SEPARATOR, dtype=str, encoding=config.FILE_ENCODING
                )
                if not df.empty:
                    try:
                        stats["clientes_insertados"] += self.db.insert_clients(df)
                    except Exception as e:
                        stats["errores"].append(f"Error en {archivo.name}: {e}")

            # Cargar tarjetas (solo si hay clientes cargados)
            if stats["clientes_insertados"] > 0:
                for archivo in archivos_tarjetas:
                    df = pd.read_csv(
                        archivo, sep=config.CSV_SEPARATOR, dtype=str, encoding=config.FILE_ENCODING
                    )
                    if not df.empty:
                        try:
                            stats["tarjetas_insertadas"] += self.db.insert_tarjetas(df)
                        except Exception as e:
                            stats["errores"].append(f"Error en {archivo.name}: {e}")

            self.logger.info(f"  ✓ Clientes insertados: {stats['clientes_insertados']}")
            self.logger.info(f"  ✓ Tarjetas insertadas: {stats['tarjetas_insertadas']}")

        except Exception as e:
            self.logger.error(f"Error en carga a BD: {e}")
            stats["errores"].append(str(e))

        return stats


# =============================================================================
# FUNCIONES DE CONVENIENCIA
# =============================================================================


def ejecutar_pipeline_clientes() -> dict:
    """Ejecuta solo el pipeline de clientes."""
    pipeline = PipelineClientes()
    return pipeline.ejecutar()


def ejecutar_pipeline_tarjetas() -> dict:
    """Ejecuta solo el pipeline de tarjetas."""
    pipeline = PipelineTarjetas()
    return pipeline.ejecutar()


def ejecutar_etl_completo(cargar_a_bd: bool = True) -> dict:
    """
    Ejecuta el proceso ETL completo.

    Args:
        cargar_a_bd: Si True, carga a base de datos

    Returns:
        dict: Resultados de la ejecución
    """
    orchestrator = ETLOrchestrator()
    return orchestrator.ejecutar_completo(cargar_a_bd)


if __name__ == "__main__":
    ejecutar_etl_completo()
