import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import os

# --- 1. CONFIGURACIÓN ---
INPUT_CSV = 'dataset_meda_derived_data.csv'
OUTPUT_PARQUET = 'dataset_meda_derived_data.parquet'
CHUNK_SIZE = 1_000_000 # Procesamos en lotes de 1 millón de filas

# --- 2. FUNCIÓN DE TRANSFORMACIÓN ---
def transformar_lote(df):
    # Convertir LTST a datetime
    if 'LTST' in df.columns:
        time_str = df['LTST'].str.split(' ').str[1]
        df['LTST_datetime'] = pd.to_datetime(time_str, format='%H:%M:%S', errors='coerce')
        df = df.drop(columns=['LTST'])

    # Manejamos 'sol' explícitamente para asegurar consistencia.
    if 'sol' in df.columns:
        df['sol'] = df['sol'].fillna(-1).astype('int16')
        
    # Forzamos que WIND_DIRECTION sea siempre float para manejar los NaN.
    if 'WIND_DIRECTION' in df.columns:
        df['WIND_DIRECTION'] = df['WIND_DIRECTION'].astype('float32')


    # Optimizamos el resto de los tipos de datos numéricos y enteros
    for col in df.columns:

        if df[col].dtype == 'float64':
            df[col] = df[col].astype('float32')
        elif df[col].dtype == 'int64':
            if df[col].max() < 32767:
                df[col] = df[col].astype('int16')
            else:
                df[col] = df[col].astype('int32')
    
    return df

# --- 3. PROCESO DE LECTURA, TRANSFORMACIÓN Y ESCRITURA ---

print(f"Iniciando la conversión de '{INPUT_CSV}' a formato Parquet optimizado...")

# Creamos un iterador para leer el CSV en lotes
csv_iter = pd.read_csv(INPUT_CSV, chunksize=CHUNK_SIZE, iterator=True)

# Tomamos el primer lote para definir la estructura del archivo Parquet
primer_lote = next(csv_iter)
primer_lote_transformado = transformar_lote(primer_lote)

# Creamos el escritor de Parquet con el esquema del primer lote transformado
writer = pq.ParquetWriter(OUTPUT_PARQUET, schema=pa.Table.from_pandas(primer_lote_transformado).schema)
writer.write_table(pa.Table.from_pandas(primer_lote_transformado))

print("Procesando el resto de los lotes...")
# Procesamos los lotes restantes
for lote in tqdm(csv_iter):
    lote_transformado = transformar_lote(lote)
    writer.write_table(pa.Table.from_pandas(lote_transformado))

writer.close()
print(f"¡Proceso completado! Archivo optimizado guardado en: '{OUTPUT_PARQUET}'")

# Comparamos tamaños de archivo
original_size = os.path.getsize(INPUT_CSV) / (1024**3)
new_size = os.path.getsize(OUTPUT_PARQUET) / (1024**3)
print(f"\nTamaño original (CSV): {original_size:.2f} GB")
print(f"Tamaño nuevo (Parquet): {new_size:.2f} GB")