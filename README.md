# Procesamiento de Datos MEDA del Rover Perseverance 



Este repositorio contiene un conjunto de scripts desarrollados en Python para el procesamiento de datos del instrumento **MEDA (Mars Environmental Dynamics Analyzer)**, a bordo del rover Perseverance de la NASA. El proyecto, que constituye un anexo de la monografía final de la materia **Astronomía II** (cátedra [Prof. Sergio R. Stürtz](https://www.linkedin.com/in/sergio-r-st%C3%BCrtz-422799249), del Profesorado en Física con sede en el  [ISFD N° 102](https://isfd102-bue.infd.edu.ar/sitio/)), tiene como objetivo consolidar y optimizar los miles de archivos de datos diarios provenientes del **[NASA Planetary Data System (PDS)](https://pds-atmospheres.nmsu.edu/data_and_services/atmospheres_data/PERSEVERANCE/meda.html)** en dos datasets maestros, depurados y eficientes, listos para su posterior análisis científico.




## Nota importante sobre los datos

Este repositorio aloja **únicamente el código fuente (`.py`)** del proyecto. Los archivos de datos, cuyo volumen asciende a varias decenas de gigabytes, **NO se incluyen en el presente repositorio**.

Para el correcto funcionamiento de los scripts, usted deberá descargar los datos de forma independiente y ubicarlos en la estructura de directorios especificada. Se recomienda de manera enfática el uso de un archivo `.gitignore` para prevenir la carga accidental de dichos datos al repositorio.



## Estructura del repositorio

```cmd
perseverance-meda-scripts/
├── .gitignore
├── README.md
├── requirements.txt
├── 1.a_create_dataset_meda_derived_data.py
├── 1.b_optimize_dataset_meda_derived_data.py
├── 2.a_create_dataset_meda_calibrated_data.py
└── 2.b_optimize_dataset_meda_calibrated_data.py
```

- **`requirements.txt`**: Archivo que especifica las librerías de Python necesarias para la ejecución del proyecto.
- **`1.a_create_dataset_meda_derived_data.py`**: Consolida los datos diarios "Derived" en un único archivo CSV masivo.
- **`1.b_optimize_dataset_meda_derived_data.py`**: Optimiza el CSV "Derived" a formato Parquet, aplicando limpieza y corrección de tipos.
- **`2.a_create_dataset_meda_calibrated_data.py`**: Consolida los datos diarios "Calibrated" en un único archivo CSV masivo.
- **`2.b_optimize_dataset_meda_calibrated_data.py`**: Optimiza el CSV "Calibrated" a formato Parquet.
- **`.gitignore`**: Archivo de configuración para que Git ignore directorios y archivos específicos.



## Prerrequisitos


1. **Python 3.9+**
2. **Datos de MEDA**:
   - Es necesario descargar los conjuntos de datos desde el portal oficial del [NASA PDS](https://pds-atmospheres.nmsu.edu/PDS/data/PDS4/Mars2020/):
     - **"Derived"** (`meda_derived.tar.gz` 2,9 GB)
     - **"Calibrated"** (`meda_calibrated.tar.gz` 10,0 GB)
   - Una vez descomprimidos (los archivos `.tar.gz`), los directorios deben ser ubicados en la raíz del proyecto, conservando la siguiente estructura:

```cmd
perseverance-meda-scripts/
├── data_calibrated_env/
│   └── sol_.../
└── data_derived_env/
    └── sol_.../
```



## Instalación

1. Clone el repositorio en su sistema local.

```bash
git clone https://github.com/la9una/perseverance-meda-scripts.git
```

2. Se recomienda la creación de un entorno virtual para gestionar las dependencias:

```bash
python -m venv mars
```
3. Active el entorno virtual:
   - En Linux/macOS: `source mars/bin/activate`
   - En Windows: `mars\Scripts\activate`

4. Instale las dependencias especificadas mediante el siguiente comando:
```bash
pip install -r requirements.txt
```



## Flujo de trabajo


El proceso se divide en dos pipelines independientes, uno para cada tipo de dato.



### Pipeline 1: Procesamiento de Datos Derivados

Este conjunto de scripts se enfoca en los datos derivados, que corresponden a variables ambientales de alto nivel, tales como presión, humedad y velocidad del viento, calculadas a partir de las mediciones primarias del instrumento.



#### **Paso 1.a: Consolidar a CSV**
Ejecute este script para unificar todos los archivos diarios de datos derivados.

```bash
python 1.a_create_dataset_meda_derived_data.py
```

- **Entrada:** Directorio `data_derived_env/`.
- **Salida:** Un archivo CSV masivo (ej: `dataset_meda_derived_data.csv`).
- ⚠️ **Advertencia:** Este proceso es computacionalmente intensivo. Se recomienda un equipo con un mínimo de **8 GB de RAM** y un procesador **Intel Core i3 / AMD Ryzen 3** o superior.



#### **Paso 1.b: Optimizar a Parquet**
Una vez generado el CSV, optimícelo al formato Parquet.

```bash
python 1.b_optimize_dataset_meda_derived_data.py
```

- **Entrada:** El archivo CSV generado en el paso anterior.
- **Salida:** `meda_derived_clean.parquet`.



### Pipeline 2: Procesamiento de Datos Calibrados

Este segundo conjunto de scripts procesa los datos calibrados, que representan las mediciones directas y de más bajo nivel de los sensores del instrumento MEDA, como por ejemplo, las lecturas de temperatura de los termopares.



#### **Paso 2.a: Consolidar a CSV**
Ejecute este script para unificar todos los archivos diarios de datos calibrados.

```bash
python 2.a_create_dataset_meda_calibrated_data.py
```

- **Entrada:** Directorio `data_calibrated_env/`.
- **Salida:** Un archivo CSV masivo (ej: `meda_calibrated_consolidado.csv`).



#### **Paso 2.b: Optimizar a Parquet**
Finalmente, optimice el CSV de datos calibrados.

```bash
python 2.b_optimize_dataset_meda_calibrated_data.py
```

- **Entrada:** El archivo CSV generado en el paso 2.a.
- **Salida:** `meda_calibrated_clean.parquet`.
- ✅ **Resultado:** Al finalizar ambos pipelines, la etapa de ingeniería de datos está completa y los archivos Parquet están listos para el análisis.



## Recursos del proyecto

A continuación, se presenta un inventario de los principales archivos del proyecto. La tabla abarca desde los recursos originales descargados hasta los datasets finales generados por los scripts de procesamiento.



| RECURSO                                         | TIPO                            | TAMAÑO  | DESCRIPCIÓN                                                              |
| ----------------------------------------------- | ------------------------------- | ------- | ------------------------------------------------------------------------ |
| `meda_derived.tar.gz`                           | Archivo comprimido              | 3,2 GB  | Colección de datos derivados, tal como fue descargada de la NASA.          |
| `data_derived_env/`                             | Carpeta con archivos originales | 20,1 GB | Datos brutos. Contiene 13.261 archivos organizados por soles.            |
| `1.a_create_dataset_meda_derived_data.py`       | Script Python                   | 5,5 KB  | Consolida los datos de `data_derived_env` en un único dataset CSV.       |
| `dataset_meda_derived_data.csv`                 | Archivo de datos consolidados   | 4,7 GB  | Dataset unificado de todos los datos derivados.                          |
| `1.b_optimize_dataset_meda_derived_data.py`     | Script Python                   | 2,8 KB  | Optimiza el CSV de datos derivados a formato Parquet.                    |
| `meda_derived_clean.parquet`                    | Archivo de datos optimizados    | 794 MB  | Versión final, comprimida y eficiente del dataset derivado.              |
| `meda_calibrated.tar.gz`                        | Archivo comprimido              | 10,7 GB | Colección de datos calibrados, tal como fue descargada de la NASA.         |
| `data_calibrated_env/`                          | Carpeta con archivos originales | 51,9 GB | Datos brutos. Contiene 18.913 archivos organizados por soles.            |
| `2.a_create_dataset_meda_calibrated_data.py`    | Script Python                   | 2,9 KB  | Consolida los datos de `data_calibrated_env` en un único dataset CSV.    |
| `dataset_meda_calibrated_data.csv`              | Archivo de datos consolidados   | 20,7 GB | Dataset unificado de los datos de los sensores ATS y RDS.                |
| `2.b_optimize_dataset_meda_calibrated_data.py`  | Script Python                   | 2,9 KB  | Optimiza el CSV de datos calibrados a formato Parquet.                   |
| `meda_calibrated_clean.parquet`                 | Archivo de datos optimizados    | 5,3 GB  | Versión final, comprimida y eficiente del dataset calibrado.             |




## Contenido del `.gitignore`

Este archivo previene la subida de datos, entornos virtuales y otros archivos innecesarios al repositorio, manteniéndolo enfocado exclusivamente en el código fuente.


```bash
# Entorno Virtual
mars/
venv/

# Directorios de Datos Crudos
data_calibrated_env/
data_derived_env/

# Archivos de Datos Comprimidos
*.tar.gz

# Archivos de Datos Generados
*.csv
*.parquet

# Caché y artefactos de Python
__pycache__/
*.pyc
```
