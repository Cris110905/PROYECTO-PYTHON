import pandas as pd
import io
import re

clientes_path = "ficheros/Clientes-2026-01-19.csv"

# Read the CSV
with open(clientes_path, 'r', encoding='latin-1') as f:
    lines = f.readlines()

cleaned_lines = [line.strip().strip('"') + '\n' for line in lines]
df = pd.read_csv(io.StringIO(''.join(cleaned_lines)), sep=";", dtype=str)

print(f"Total clientes en CSV: {len(df)}")
print(f"\nPrimeros 10 cod_cliente:")
print(df['cod_cliente'].head(10).tolist())

# Check which ones pass the name validation
nameRegex = r"^[a-zA-ZáéíóúÁÉÍÓÚñÑ\s]+$"
valid_count = 0
invalid_count = 0

for idx, row in df.iterrows():
    if re.fullmatch(nameRegex, str(row['nombre'])):
        valid_count += 1
    else:
        invalid_count += 1
        if invalid_count <= 5:
            print(f"  Nombre inválido: '{row['nombre']}' (cod_cliente: {row['cod_cliente']})")

print(f"\nClientes válidos: {valid_count}")
print(f"Clientes inválidos (filtrados): {invalid_count}")
