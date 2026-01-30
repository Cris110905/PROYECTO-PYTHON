@echo off
echo Iniciando el Programador Automatico ETL...
echo Esta ventana debe permanecer abierta para que la programacion funcione.
echo Puedes minimizarla si lo deseas.

:: Navegar al directorio donde se encuentra este script
cd /d "%~dp0"

:: Verificar si existe el entorno virtual y activarlo
if exist "venv\Scripts\activate.bat" (
    echo Activando entorno virtual...
    call venv\Scripts\activate.bat
) else (
    echo No se encontro 'venv'. Se intentara usar el Python instalado en el sistema.
)

:: Ejecutar el script de automatización
python src\automation.py

pause
