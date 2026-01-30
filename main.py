import os
import re
import pandas as pd
import dropbox
from dotenv import load_dotenv
from database import get_engine

load_dotenv()
TOKEN = os.getenv("DROPBOX_TOKEN")
dbx = dropbox.Dropbox(TOKEN)

def norm_texto(valor):
    return str(valor).strip().title()

def norm_dni(valor):
    return str(valor).strip().upper()

def norm_correo(valor):
    return str(valor).strip().lower()

def norm_telefono(valor):
    return re.sub(r"\D", "", str(valor))

def norm_numero_tarjeta(valor):
    return re.sub(r"\s+", "", str(valor))

CLIENTES_NORMALIZERS = {
    "nombre": norm_texto,
    "apellido1": norm_texto,
    "apellido2": norm_texto,
    "dni": norm_dni,
    "correo": norm_correo,
    "telefono": norm_telefono
}

CARD_NORMALIZERS = {
    "numero_tarjeta": norm_numero_tarjeta,
    "cvv": lambda x: str(x).strip(),
    "fecha_exp": lambda x: str(x).strip()
}

nameRegex = r"^[a-zA-ZáéíóúÁÉÍÓÚñÑ\s]+$"

def descargar_archivo(client, dropboxPath, localPath):
    # Skip download if file already exists locally
    if os.path.exists(localPath):
        print(f"Usando archivo local existente: {localPath}")
        return
    try:
        os.makedirs(os.path.dirname(localPath), exist_ok=True)
        metadata, res = client.files_download(path=dropboxPath)
        with open(localPath, 'wb') as f:
            f.write(res.content)
        print(f"Archivo descargado: {localPath}")
    except Exception as e:
        print(f"Error descargando {dropboxPath}: {e}")

# Use existing CSV files from ficheros directory
clientes_path = "ficheros/Clientes-2026-01-19.csv"
tarjetas_path = "ficheros/Tarjetas-2026-01-19.csv"

# Try to download only if files don't exist
if not os.path.exists(clientes_path):
    descargar_archivo(dbx, "/Clientes-2026-01-19.csv", clientes_path)
if not os.path.exists(tarjetas_path):
    descargar_archivo(dbx, "/Tarjetas-2026-01-19.csv", tarjetas_path)

try:
    # Both CSV files have rows wrapped in quotes - need to remove them first
    import io
    
    # Process clientes file
    with open(clientes_path, 'r', encoding='latin-1') as f:
        lines_c = f.readlines()
    cleaned_lines_c = [line.strip().strip('"') + '\n' for line in lines_c]
    df_c = pd.read_csv(io.StringIO(''.join(cleaned_lines_c)), sep=";", dtype=str)
    
    # Process tarjetas file
    with open(tarjetas_path, 'r', encoding='latin-1') as f:
        lines_t = f.readlines()
    cleaned_lines_t = [line.strip().strip('"') + '\n' for line in lines_t]
    df_t = pd.read_csv(io.StringIO(''.join(cleaned_lines_t)), sep=";", dtype=str)
except FileNotFoundError:
    df_c, df_t = pd.DataFrame(), pd.DataFrame()

def procesar_datos(df, tipo):
    if df.empty: return pd.DataFrame()
    datos_procesados = []
    headers = df.columns.tolist()
    normalizadores = CLIENTES_NORMALIZERS if tipo == "Clientes" else CARD_NORMALIZERS
    
    for _, fila in df.iterrows():
        nueva_fila = {}
        fila_valida = True
        for i, valor in enumerate(fila):
            columna = headers[i]
            if tipo == "Clientes" and columna == "nombre":
                if not re.fullmatch(nameRegex, str(valor)):
                    fila_valida = False
                    break
            if columna in normalizadores:
                nueva_fila[columna] = normalizadores[columna](valor)
            else:
                nueva_fila[columna] = valor
        if fila_valida:
            datos_procesados.append(nueva_fila)
    return pd.DataFrame(datos_procesados)

df_clientes_final = procesar_datos(df_c, "Clientes")
df_tarjetas_final = procesar_datos(df_t, "Tarjetas")

if not df_clientes_final.empty:
    try:
        engine = get_engine()
        # Insert clients in a separate transaction
        with engine.begin() as conn:
            df_clientes_final.to_sql('clients', con=conn, if_exists='append', index=False)
            print(f"[OK] {len(df_clientes_final)} registros de clientes insertados correctamente en la tabla 'clients'")
        
        # Filter tarjetas to only include cards for existing clients
        if not df_tarjetas_final.empty:
            valid_cod_clientes = set(df_clientes_final['cod_cliente'].tolist())
            df_tarjetas_filtered = df_tarjetas_final[df_tarjetas_final['cod_cliente'].isin(valid_cod_clientes)]
            
            if not df_tarjetas_filtered.empty:
                with engine.begin() as conn:
                    df_tarjetas_filtered.to_sql('tarjetas', con=conn, if_exists='append', index=False)
                    print(f"[OK] {len(df_tarjetas_filtered)} registros de tarjetas insertados correctamente en la tabla 'tarjetas'")
                
                skipped = len(df_tarjetas_final) - len(df_tarjetas_filtered)
                if skipped > 0:
                    print(f"[INFO] {skipped} tarjetas omitidas (sin cliente correspondiente)")
        
        print("\n[OK] Datos cargados exitosamente a PostgreSQL!")
    except Exception as e:
        print(f"[ERROR] Error al cargar datos a PostgreSQL: {e}")
else:
    print("No hay datos de clientes para cargar.")