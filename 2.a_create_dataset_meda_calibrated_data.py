import pandas as pd
import os
import glob
from tqdm import tqdm

# ---

## 1. Configuración inicial y almacenamiento de datos 

# Definimos la ruta a la carpeta principal de los datos calibrados.
base_path = 'data_calibrated_env' 
output_file = 'dataset_meda_calibrated_data.csv'
BATCH_SIZE = 50
header_written = False

# ---

## 2. Búsqueda y consolidación por lotes 

print("Paso 1: Buscando todos los archivos de datos.")

# Buscamos de forma recursiva los archivos del sensor ATS, nuestra base.
all_ats_files = glob.glob(os.path.join(base_path, '**', '*CAL_ATS*P*.CSV'), recursive=True)

if not all_ats_files:
    print("No se encontraron archivos para procesar. Verifica el nombre de la carpeta.")
else:
    # También buscamos los archivos del sensor RDS para contarlos.
    all_rds_files = glob.glob(os.path.join(base_path, '**', '*CAL_RDS*P*.CSV'), recursive=True)

    print("Búsqueda completa. Encontrados:")
    print(f"- {len(all_ats_files)} archivos de ATS.")
    print(f"- {len(all_rds_files)} archivos de RDS.")
    total_files = len(all_ats_files)

    print("Paso 2: Iniciando el procesamiento de lotes. Esto demorará algunos minutos...")

    for i in tqdm(range(0, total_files, BATCH_SIZE), desc="Procesando lotes"):
        batch_files = all_ats_files[i:i + BATCH_SIZE]
        list_of_batch_dfs = []
        
        for ats_file_path in batch_files:
            try:
                df_ats = pd.read_csv(ats_file_path)
                df_ats['SCLK'] = df_ats['SCLK'].astype(float)
                
                sol_number = os.path.basename(ats_file_path).split('__')[1]
                sol_path = os.path.dirname(ats_file_path)
                
                rds_files = glob.glob(os.path.join(sol_path, f'WE__{sol_number}*CAL_RDS*P*.CSV'))
                
                if rds_files:
                    df_rds = pd.read_csv(rds_files[0])
                    df_rds['SCLK'] = df_rds['SCLK'].astype(float)
                    
                    merged_df = pd.merge(df_ats, df_rds, on='SCLK', how='outer', suffixes=('_ats', '_rds'))
                    
                    merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]
                    
                    merged_df['sol'] = sol_number
                    list_of_batch_dfs.append(merged_df)
                else:
                    df_ats['sol'] = sol_number
                    list_of_batch_dfs.append(df_ats)

            except Exception as e:
                tqdm.write(f"Error al procesar los archivos en {sol_path}: {e}")

        if list_of_batch_dfs:
            batch_df = pd.concat(list_of_batch_dfs, ignore_index=True)
            
            if not header_written:
                batch_df.to_csv(output_file, index=False, mode='w', header=True)
                header_written = True
            else:
                batch_df.to_csv(output_file, index=False, mode='a', header=False)

# ---

## 3. Mensaje final

print("\n¡Dataset consolidado creado exitosamente! 🎉")
