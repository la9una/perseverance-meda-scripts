"""
## 0. Importación de librerías
"""
import pandas as pd
import os
import glob
from tqdm import tqdm

"""
## 1. Configuración inicial para lotes
"""
# Aquí defines la ruta a la carpeta principal de los datos derivados.
base_path = 'data_derived_env'

# --- NUEVA CONFIGURACIÓN PARA LOTES ---
# Nombre del archivo final que se creará.
output_file = 'dataset_meda_derived_data.csv'
# Número de archivos de "sol" a procesar en cada lote.
# Puedes ajustar este número según la RAM de tu equipo. 50 es un valor seguro.
BATCH_SIZE = 50
# Bandera para asegurarnos de que el encabezado se escriba solo una vez.
header_written = False
# --- FIN DE LA NUEVA CONFIGURACIÓN ---

"""
## 2. Búsqueda y consolidación por lotes
"""
print("Paso 1: Buscando todos los archivos ANCILLARY.")

# Usamos 'glob' para encontrar de forma recursiva todos los archivos 'ANCILLARY'.
all_ancillary_files = glob.glob(os.path.join(base_path, '**', '*DER_ANCILLARY*P*.CSV'), recursive=True)

# Verificamos si encontramos algún archivo.
if not all_ancillary_files:
    print("No se encontraron archivos ANCILLARY. Por favor, verifica la ruta de la carpeta.")
else:
    total_files = len(all_ancillary_files)
    print(f"Búsqueda completa. Se encontraron {total_files} archivos ANCILLARY.")
    print("Paso 2: Iniciando el procesamiento por lotes. Esto demorará algunos minutos...")

    # --- NUEVO BUCLE PRINCIPAL POR LOTES ---
    # Este bucle itera sobre la lista de archivos en trozos del tamaño de BATCH_SIZE.
    for i in tqdm(range(0, total_files, BATCH_SIZE), desc="Procesando lotes"):
        batch_files = all_ancillary_files[i:i + BATCH_SIZE]
        list_of_batch_dfs = [] # Lista temporal solo para este lote.

        # Este bucle interno procesa cada archivo dentro del lote actual.
        for file_path in batch_files:
            try:
                # --- El código de procesamiento de un sol individual se mantiene igual ---
                df_ancillary = pd.read_csv(file_path)
                sol_number = os.path.basename(file_path).split('__')[1]
                sol_path = os.path.dirname(file_path)
                df_ancillary['SCLK'] = df_ancillary['SCLK'].astype(float)
                sol_df = df_ancillary[['SCLK', 'LTST', 'SOLAR_ZENITHAL_ANGLE']]

                # PRESIÓN (PS)
                ps_files = glob.glob(os.path.join(sol_path, f'WE__{sol_number}*DER_PS*P*.CSV'))
                if ps_files:
                    df_ps = pd.read_csv(ps_files[0])
                    df_ps['SCLK'] = df_ps['SCLK'].astype(float)
                    sol_df = pd.merge(sol_df, df_ps[['SCLK', 'PRESSURE']], on='SCLK', how='left')

                # HUMEDAD (RHS)
                rhs_files = glob.glob(os.path.join(sol_path, f'WE__{sol_number}*DER_RHS*P*.CSV'))
                if rhs_files:
                    df_rhs = pd.read_csv(rhs_files[0])
                    df_rhs['SCLK'] = df_rhs['SCLK'].astype(float)
                    sol_df = pd.merge(sol_df, df_rhs[['SCLK', 'LOCAL_RELATIVE_HUMIDITY', 'HUMIDITY_LOCAL_TEMP']], on='SCLK', how='left')

                # TEMPERATURA Y RADIACIÓN (TIRS)
                try:
                    tirs_files = glob.glob(os.path.join(sol_path, f'WE__{sol_number}*DER_TIRS*P*.CSV'))
                    if tirs_files:
                        df_tirs = pd.read_csv(tirs_files[0])
                        df_tirs['SCLK'] = df_tirs['SCLK'].astype(float)
                        tirs_cols_to_merge = ['SCLK']
                        if 'SURFACE_TEMPERATURE' in df_tirs.columns: tirs_cols_to_merge.append('SURFACE_TEMPERATURE')
                        if 'UPWARD_LW_IRRADIANCE' in df_tirs.columns: tirs_cols_to_merge.append('UPWARD_LW_IRRADIANCE')
                        if 'DOWNWARD_LW_IRRADIANCE' in df_tirs.columns: tirs_cols_to_merge.append('DOWNWARD_LW_IRRADIANCE')
                        if len(tirs_cols_to_merge) > 1:
                            sol_df = pd.merge(sol_df, df_tirs[tirs_cols_to_merge], on='SCLK', how='left')
                except Exception as e:
                    tqdm.write(f"Error al procesar TIRS para {sol_number}: {e}")

                # VIENTO (WIND)
                ws_files = glob.glob(os.path.join(sol_path, f'WE__{sol_number}*DER_WS*P*.CSV'))
                if ws_files:
                    df_ws = pd.read_csv(ws_files[0])
                    df_ws['SCLK'] = df_ws['SCLK'].astype(float)
                    df_ws.replace(999999999, float('NaN'), inplace=True)
                    sol_df = pd.merge(sol_df, df_ws[['SCLK', 'HORIZONTAL_WIND_SPEED', 'WIND_DIRECTION']], on='SCLK', how='left')

                # Agregamos la columna 'sol' y lo añadimos a la lista del LOTE.
                sol_df['sol'] = sol_number
                list_of_batch_dfs.append(sol_df) # Cambio: se añade a la lista del lote.

            except Exception as e:
                tqdm.write(f"Error al procesar el archivo {file_path}: {e}")

        # --- NUEVO BLOQUE DE ESCRITURA POR LOTES ---
        # Al final de cada lote, si hemos procesado datos, los escribimos en el CSV.
        if list_of_batch_dfs:
            batch_df = pd.concat(list_of_batch_dfs, ignore_index=True)
            
            # Si es el primer lote, escribe el archivo con encabezado.
            if not header_written:
                batch_df.to_csv(output_file, index=False, mode='w', header=True)
                header_written = True
            # Para los siguientes lotes, añade los datos sin el encabezado.
            else:
                batch_df.to_csv(output_file, index=False, mode='a', header=False)

"""
## 3. Mensaje final
"""
print("\n¡Dataset consolidado creado exitosamente! 🎉")
