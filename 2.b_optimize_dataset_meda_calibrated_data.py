import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import os

# --- 1. CONFIGURACIÓN ---
INPUT_CSV = 'dataset_meda_calibrated_data.csv'
OUTPUT_PARQUET = 'dataset_meda_calibrated_data.parquet'
CHUNK_SIZE = 1_000_000

# --- 2. CREACIÓN DE UN ESQUEMA DE TIPOS DE DATOS FIJO ---
print("Definiendo un esquema de tipos de datos fijo para garantizar la consistencia...")
try:
    column_names = pd.read_csv(INPUT_CSV, nrows=0).columns.tolist()
    dtype_map = {}
    for col in column_names:
        if 'LMST' in col or 'LTST' in col:
            dtype_map[col] = str
        elif col != 'sol':
            dtype_map[col] = 'float32'
    print("Esquema definido con éxito.")
except FileNotFoundError:
    print(f"Error: No se encontró el archivo '{INPUT_CSV}'. Verifica el nombre y la ubicación.")
    exit()

# --- 3. FUNCIÓN DE TRANSFORMACIÓN ---
def transformar_lote_calibrado(df):
    for col_name in ['LMST_ats', 'LTST_ats']:
        if col_name in df.columns:
            time_str = df[col_name].str.split('M').str[1]
            df[col_name + '_datetime'] = pd.to_datetime(time_str, format='%H:%M:%S.%f', errors='coerce')
            df = df.drop(columns=[col_name])
            
    for col_name in ['LMST_rds', 'LTST_rds']:
        if col_name in df.columns:
            df[col_name] = df[col_name].fillna('').astype(str)

    if 'sol' in df.columns:
        df['sol'] = df['sol'].fillna(-1).astype('int16')
    
    return df

# --- 4. PROCESO DE LECTURA CON ESQUEMA FIJO Y ESCRITURA FORZADA ---

print(f"Iniciando la conversión de '{INPUT_CSV}' a formato Parquet optimizado...")

csv_iter = pd.read_csv(
    INPUT_CSV, 
    chunksize=CHUNK_SIZE, 
    iterator=True, 
    engine='python',
    on_bad_lines='skip',
    dtype=dtype_map
)

primer_lote = next(csv_iter)
primer_lote_transformado = transformar_lote_calibrado(primer_lote)

# Creamos el escritor y capturamos el esquema maestro
writer = pq.ParquetWriter(OUTPUT_PARQUET, schema=pa.Table.from_pandas(primer_lote_transformado).schema)
writer.write_table(pa.Table.from_pandas(primer_lote_transformado))

# --- ESQUEMA MAESTRO ---
# Guardamos el esquema maestro para usarlo como plantilla
master_schema = writer.schema
# --- FIN DEL ESQUEMA MAESTRO ---

print("Procesando el resto de los lotes...")
for lote in tqdm(csv_iter):
    lote_transformado = transformar_lote_calibrado(lote)
    
    # --- CONVERSION A PYARROW ---
    # Convertimos el DataFrame a una Tabla de PyArrow, forzando el uso del esquema maestro.
    tabla_lote = pa.Table.from_pandas(lote_transformado, schema=master_schema)
    writer.write_table(tabla_lote)
    # --- FIN DE CONVERSION ---

writer.close()
print(f"¡Proceso completado! Archivo optimizado guardado en: '{OUTPUT_PARQUET}'")

original_size = os.path.getsize(INPUT_CSV) / (1024**3)
new_size = os.path.getsize(OUTPUT_PARQUET) / (1024**3)
print(f"\nTamaño original (CSV): {original_size:.2f} GB")
print(f"Tamaño nuevo (Parquet): {new_size:.2f} GB")