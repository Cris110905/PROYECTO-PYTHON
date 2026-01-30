import pandas as pd
import io

clientes_path = "ficheros/Clientes-2026-01-19.csv"

with open(clientes_path, 'r', encoding='latin-1') as f:
    lines = f.readlines()

cleaned_lines = [line.strip().strip('"') + '\n' for line in lines]
df = pd.read_csv(io.StringIO(''.join(cleaned_lines)), sep=";", dtype=str, nrows=3)

print('Columnas:', df.columns.tolist())
print('\nPrimeras filas:')
print(df.head())
