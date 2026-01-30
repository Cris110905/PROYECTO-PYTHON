import pandas as pd
import io

tarjetas_path = "ficheros/Tarjetas-2026-01-19.csv"

# Tarjetas file has each row wrapped in quotes - need to remove them first
with open(tarjetas_path, 'r', encoding='latin-1') as f:
    lines = f.readlines()

# Remove quotes from each line
cleaned_lines = [line.strip().strip('"') + '\n' for line in lines]

# Write to temporary file and read with pandas
df_t = pd.read_csv(io.StringIO(''.join(cleaned_lines)), sep=";", dtype=str)

print('Columnas:', df_t.columns.tolist())
print('\nPrimeras filas:')
print(df_t.head())
