import pandas as pd
import pyarrow.parquet as pq # Importante: importamos pyarrow.parquet

# Nombres de tus archivos
file_derived = 'dataset_meda_derived_data.parquet'
file_calibrated = 'dataset_meda_calibrated_data.parquet'

# --- Inspección SEGURA del Dataset Derivado ---
print("--- Cabecera del Dataset Derivado (794 MB) ---")

# 1. Abrimos una referencia al archivo, sin cargarlo en memoria
parquet_file_derived = pq.ParquetFile(file_derived)
# 2. Leemos solo el primer "grupo de filas" (un trozo del archivo)
primer_trozo_derived = parquet_file_derived.read_row_group(0)
# 3. Convertimos ESE TROZO a un DataFrame de pandas
df_derived_sample = primer_trozo_derived.to_pandas()

# 4. Ahora sí, inspeccionamos la muestra de forma segura
print(f"Muestra leída con {len(df_derived_sample)} filas.")
df_derived_sample.info()

print("\n" + "="*50 + "\n")

# --- Inspección SEGURA del Dataset Calibrado (el más pesado) ---
print("--- Cabecera del Dataset Calibrado (5.3 GB) ---")

parquet_file_calibrated = pq.ParquetFile(file_calibrated)
primer_trozo_calibrated = parquet_file_calibrated.read_row_group(0)
df_calibrated_sample = primer_trozo_calibrated.to_pandas()

print(f"Muestra leída con {len(df_calibrated_sample)} filas.")
df_calibrated_sample.info()