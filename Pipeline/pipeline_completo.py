import pandas as pd
import re
from pathlib import Path
from datetime import datetime
import config
import utils

class PipelineBase:
    def __init__(self, logger_name, log_file, stats_key):
        self.logger = utils.setup_logger(logger_name, log_file)
        self.stats = {
            'filas_leidas': 0,
            'filas_procesadas': 0,
            'filas_rechazadas': 0,
            'archivos_procesados': 0
        }
        self.stats_key = stats_key

    def detectar_archivos(self, pattern):
        archivos_encontrados = []
        self.logger.info(f"Buscando archivos ({pattern}) en directorio input/")
        
        for archivo in config.INPUT_DIR.glob('*.csv'):
            if re.match(pattern, archivo.name):
                archivos_encontrados.append(archivo)
                self.logger.info(f"  ✓ Archivo válido encontrado: {archivo.name}")
            else:
                pass 
                
        if not archivos_encontrados:
            self.logger.warning(f"No se encontraron archivos para {self.stats_key}")
            
        return archivos_encontrados

    def leer_csv(self, archivo):
        try:
            self.logger.info(f"Leyendo archivo: {archivo.name}")
            df = pd.read_csv(
                archivo,
                sep=config.CSV_SEPARATOR,
                encoding=config.FILE_ENCODING,
                dtype=str,
                na_values=['', 'NULL', 'null', 'None', 'NA'],
                keep_default_na=True
            )
            self.logger.info(f"  ✓ Leídas {len(df)} filas, {len(df.columns)} columnas")
            self.stats['filas_leidas'] += len(df)
            return df
        except UnicodeDecodeError:
            self.logger.error(f"Error de codificación en {archivo.name}. Intentando con latin-1...")
            try:
                df = pd.read_csv(
                    archivo,
                    sep=config.CSV_SEPARATOR,
                    encoding='latin-1',
                    dtype=str
                )
                self.logger.info(f"  ✓ Archivo leído con encoding latin-1")
                return df
            except Exception as e:
                self.logger.error(f"Error al leer {archivo.name} (latin-1): {str(e)}")
                return None
        except Exception as e:
            self.logger.error(f"Error al leer {archivo.name}: {str(e)}")
            return None

    def limpiar_datos(self, df):
        self.logger.info("Iniciando limpieza de datos")
        df_limpio = df.copy()
        
        self.logger.info("  - Eliminando espacios en blanco")
        for col in df_limpio.columns:
            if df_limpio[col].dtype == 'object':
                df_limpio[col] = df_limpio[col].str.strip()
        
        self.logger.info("  - Normalizando nombres de columnas")
        nuevos_nombres = {}
        for col in df_limpio.columns:
            nuevo_nombre = col.strip().lower()
            nuevo_nombre = utils.eliminar_acentos(nuevo_nombre)
            nuevo_nombre = nuevo_nombre.replace(' ', '_')
            nuevos_nombres[col] = nuevo_nombre
        
        df_limpio.rename(columns=nuevos_nombres, inplace=True)
        df_limpio.fillna('', inplace=True)
        return df_limpio

    def normalizar_datos(self, df):
        return df

    def validar_datos(self, df):
        return df

    def identificar_filas_rechazadas_base(self, df, campos_obligatorios):
        mascara_valida = pd.Series([True] * len(df))
        motivos_rechazo = [''] * len(df)
        
        for campo in campos_obligatorios:
            col_verificar = campo
            if f"{campo}_limpio" in df.columns:
                 col_verificar = f"{campo}_limpio"
            
            if col_verificar in df.columns:
                mascara_campo = df[col_verificar].notna() & (df[col_verificar] != '')
                mascara_invalida = ~mascara_campo
                
                if mascara_invalida.any():
                    for idx in df[mascara_invalida].index:
                        if motivos_rechazo[idx]:
                            motivos_rechazo[idx] += f"; {campo} vacío"
                        else:
                            motivos_rechazo[idx] = f"{campo} vacío"
                    mascara_valida &= mascara_campo
        
        df['motivo_rechazo'] = motivos_rechazo
        df_valido = df[mascara_valida].copy()
        df_rechazado = df[~mascara_valida].copy()
        
        if len(df_rechazado) > 0:
            self.logger.warning(f"  ⚠ {len(df_rechazado)} filas rechazadas por validación")
            self.stats['filas_rechazadas'] += len(df_rechazado)
            
        return df_valido, df_rechazado

    def identificar_filas_rechazadas(self, df):
        raise NotImplementedError

    def anonimizar_datos(self, df):
        return df

    def guardar_resultados(self, df_valido, df_rechazado, archivo_original):
        raise NotImplementedError

    def ejecutar(self, pattern):
        self.logger.info("="*80)
        self.logger.info(f"INICIANDO PIPELINE: {self.stats_key.upper()}")
        self.logger.info("="*80)
        
        inicio = datetime.now()
        archivos = self.detectar_archivos(pattern)
        
        if not archivos:
            self.logger.info("No hay archivos para procesar.")
            return

        for archivo in archivos:
            self.logger.info("="*80)
            self.logger.info(f"Procesando: {archivo.name}")
            self.logger.info("="*80)
            
            df = self.leer_csv(archivo)
            if df is None:
                continue
            
            df = self.limpiar_datos(df)
            df = self.normalizar_datos(df)
            df = self.validar_datos(df)
            
            df_valido, df_rechazado = self.identificar_filas_rechazadas(df)
            df_valido = self.anonimizar_datos(df_valido)
            
            self.guardar_resultados(df_valido, df_rechazado, archivo)
            self.stats['archivos_procesados'] += 1
            
        fin = datetime.now()
        duracion = (fin - inicio).total_seconds()
        
        self.logger.info("="*80)
        self.logger.info(f"PIPELINE {self.stats_key.upper()} COMPLETADO")
        self.logger.info("="*80)
        self.logger.info(f"Archivos procesados: {self.stats['archivos_procesados']}")
        self.logger.info(f"Filas leídas: {self.stats['filas_leidas']}")
        self.logger.info(f"Filas procesadas correctamente: {self.stats['filas_procesadas']}")
        self.logger.info(f"Filas rechazadas: {self.stats['filas_rechazadas']}")
        self.logger.info(f"Tiempo de ejecución: {duracion:.2f} segundos")
        self.logger.info("="*80)


class PipelineClientes(PipelineBase):
    def __init__(self):
        super().__init__('pipeline_clientes', 'clientes_pipeline.log', 'Clientes')

    def normalizar_datos(self, df):
        self.logger.info("Normalizando datos")
        if 'nombre' in df.columns:
            self.logger.info("  - Normalizando nombres")
            df['nombre'] = df['nombre'].apply(lambda x: x.title() if x else x)
            df['nombre'] = df['nombre'].apply(utils.eliminar_acentos)
        
        for col in ['apellido1', 'apellido2']:
            if col in df.columns:
                self.logger.info(f"  - Normalizando {col}")
                df[col] = df[col].str.upper()
                df[col] = df[col].apply(utils.eliminar_acentos)
        
        if 'correo' in df.columns:
            self.logger.info("  - Normalizando correos")
            df['correo'] = df['correo'].str.lower()
        
        if 'dni' in df.columns:
            self.logger.info("  - Normalizando DNIs")
            df['dni'] = df['dni'].apply(utils.normalizar_dni)
        
        if 'telefono' in df.columns:
            self.logger.info("  - Normalizando teléfonos")
            df['telefono'] = df['telefono'].apply(utils.normalizar_telefono)
        return df

    def validar_datos(self, df):
        self.logger.info("Aplicando validaciones")
        if 'dni' in df.columns:
            self.logger.info("  - Validando DNIs")
            df['dni_valido'] = df['dni'].apply(utils.validar_dni)
            df['dni_ok'] = df['dni_valido'].apply(lambda x: 'Y' if x else 'N')
            df['dni_ko'] = df['dni_valido'].apply(lambda x: 'N' if x else 'Y')
        
        if 'telefono' in df.columns:
            self.logger.info("  - Validando teléfonos")
            df['telefono_valido'] = df['telefono'].apply(utils.validar_telefono)
            df['telefono_ok'] = df['telefono_valido'].apply(lambda x: 'Y' if x else 'N')
            df['telefono_ko'] = df['telefono_valido'].apply(lambda x: 'N' if x else 'Y')

        if 'correo' in df.columns:
            self.logger.info("  - Validando correos")
            df['correo_valido'] = df['correo'].apply(utils.validar_correo)
            df['correo_ok'] = df['correo_valido'].apply(lambda x: 'Y' if x else 'N')
            df['correo_ko'] = df['correo_valido'].apply(lambda x: 'N' if x else 'Y')
        return df

    def identificar_filas_rechazadas(self, df):
        return self.identificar_filas_rechazadas_base(df, ['cod_cliente', 'correo'])

    def anonimizar_datos(self, df):
        self.logger.info("Aplicando anonimización a datos sensibles")
        if 'dni' in df.columns:
            df['dni_hash'] = df['dni'].apply(utils.hash_con_salt)
            self.logger.info("  - DNI hasheado")
        return df

    def guardar_resultados(self, df_valido, df_rechazado, archivo_original):
        fecha = utils.extraer_fecha_archivo(archivo_original.name)
        
        if len(df_valido) > 0:
            cols_exportar = [c for c in df_valido.columns if c not in ['dni_valido', 'telefono_valido', 'correo_valido', 'motivo_rechazo']]
            archivo_salida = config.OUTPUT_DIR / f"Clientes-{fecha}.cleaned.csv"
            df_valido[cols_exportar].to_csv(
                archivo_salida, sep=config.CSV_SEPARATOR, index=False, encoding=config.FILE_ENCODING
            )
            self.logger.info(f"  ✓ Guardadas {len(df_valido)} filas válidas en: {archivo_salida.name}")
            self.stats['filas_procesadas'] += len(df_valido)
        
        if len(df_rechazado) > 0:
            archivo_errores = config.ERRORS_DIR / f"Clientes-{fecha}.rejected.csv"
            df_rechazado.to_csv(
                archivo_errores, sep=config.CSV_SEPARATOR, index=False, encoding=config.FILE_ENCODING
            )
            self.logger.info(f"  ⚠ Guardadas {len(df_rechazado)} filas rechazadas en: {archivo_errores.name}")


class PipelineTarjetas(PipelineBase):
    def __init__(self):
        super().__init__('pipeline_tarjetas', 'tarjetas_pipeline.log', 'Tarjetas')

    def normalizar_datos(self, df):
        self.logger.info("Normalizando datos")
        if 'numero_tarjeta' in df.columns:
            self.logger.info("  - Normalizando números de tarjeta")
            df['numero_tarjeta_limpio'] = df['numero_tarjeta'].apply(
                lambda x: ''.join(filter(str.isdigit, str(x))) if x else ''
            )
        if 'cvv' in df.columns:
            self.logger.info("  - Normalizando CVV")
            df['cvv_limpio'] = df['cvv'].apply(
                lambda x: ''.join(filter(str.isdigit, str(x))) if x else ''
            )
        if 'fecha_exp' in df.columns:
            self.logger.info("  - Normalizando fechas de expiración")
            df['fecha_exp'] = df['fecha_exp'].str.strip()
        return df

    def validar_datos(self, df):
        self.logger.info("Aplicando validaciones")
        if 'fecha_exp' in df.columns:
            self.logger.info("  - Validando fechas de expiración")
            df['fecha_exp_valida'] = df['fecha_exp'].apply(utils.validar_fecha_expiracion)
            df['fecha_exp_ok'] = df['fecha_exp_valida'].apply(lambda x: 'Y' if x else 'N')
            df['fecha_exp_ko'] = df['fecha_exp_valida'].apply(lambda x: 'N' if x else 'Y')
        return df

    def identificar_filas_rechazadas(self, df):
        return self.identificar_filas_rechazadas_base(df, ['cod_cliente', 'numero_tarjeta'])

    def anonimizar_datos(self, df):
        self.logger.info("Aplicando anonimización OBLIGATORIA a tarjetas")
        if 'numero_tarjeta_limpio' in df.columns:
            df['numero_tarjeta_masked'] = df['numero_tarjeta_limpio'].apply(utils.enmascarar_tarjeta)
            self.logger.info("  ✓ Número de tarjeta enmascarado")
        if 'numero_tarjeta_limpio' in df.columns:
            df['numero_tarjeta_hash'] = df['numero_tarjeta_limpio'].apply(utils.hash_con_salt)
            self.logger.info("  ✓ Número de tarjeta hasheado")
        if 'cvv_limpio' in df.columns:
            df['cvv_hash'] = df['cvv_limpio'].apply(utils.hash_con_salt)
            self.logger.info("  ✓ CVV hasheado")
        return df

    def guardar_resultados(self, df_valido, df_rechazado, archivo_original):
        fecha = utils.extraer_fecha_archivo(archivo_original.name)
        
        if len(df_valido) > 0:
            cols_sensibles = ['numero_tarjeta', 'numero_tarjeta_limpio', 'cvv', 'cvv_limpio']
            cols_internas = ['fecha_exp_valida', 'motivo_rechazo']
            cols_exportar = [c for c in df_valido.columns 
                           if c not in cols_sensibles and c not in cols_internas]
            
            archivo_salida = config.OUTPUT_DIR / f"Tarjetas-{fecha}.cleaned.csv"
            df_valido[cols_exportar].to_csv(
                archivo_salida, sep=config.CSV_SEPARATOR, index=False, encoding=config.FILE_ENCODING
            )
            self.logger.info(f"  ✓ Guardadas {len(df_valido)} filas válidas en: {archivo_salida.name}")
            self.stats['filas_procesadas'] += len(df_valido)
            
        if len(df_rechazado) > 0:
            archivo_errores = config.ERRORS_DIR / f"Tarjetas-{fecha}.rejected.csv"
            cols_sensibles = ['numero_tarjeta', 'numero_tarjeta_limpio', 'cvv', 'cvv_limpio']
            cols_exportar_rechazadas = [c for c in df_rechazado.columns if c not in cols_sensibles]
            
            df_rechazado[cols_exportar_rechazadas].to_csv(
                archivo_errores, sep=config.CSV_SEPARATOR, index=False, encoding=config.FILE_ENCODING
            )
            self.logger.info(f"  ⚠ Guardadas {len(df_rechazado)} filas rechazadas en: {archivo_errores.name}")


def main():
    print("Iniciando Pipeline Completo...")
    
    pipeline_clientes = PipelineClientes()
    pipeline_clientes.ejecutar(config.CLIENTES_PATTERN)
    
    pipeline_tarjetas = PipelineTarjetas()
    pipeline_tarjetas.ejecutar(config.TARJETAS_PATTERN)
    
    print("\nProceso completo. Ver logs para detalles.")

if __name__ == "__main__":
    main()
