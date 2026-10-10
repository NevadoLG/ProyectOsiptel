# Proyecto OSIPTEL | Big Data y Analítica de Datos

Pipeline on-premise desarrollado en Debian para procesar datos públicos de OSIPTEL** e INEI, conservar la trazabilidad de las fuentes, aplicar controles de calidad y generar indicadores trimestrales integrados mediante HDFS, Apache Spark y YARN.

## Objetivo y alcance

El proyecto prepara datos de conexiones de internet fijo reportadas por OSIPTEL y de hogares con acceso a internet publicados por INEI, para estudiar su evolución conjunta en Perú. La integración se realiza a nivel nacional, por año y trimestre.

## Tecnologías

- **Debian** en máquina virtual VirtualBox (implementación local).
- **Python 3** para extracción, preparación, validaciones auxiliares y automatización.
- **Hadoop HDFS** como Data Lake, **YARN** para la ejecución y **Apache Spark 3.5.1 / PySpark** para Silver y Gold.
- **Parquet + Snappy** como formato analítico.
- **Poppler (`pdftotext`)** y **Tesseract OCR** para PDF.
- **JupyterLab** para exploración y próximos análisis.
- **Git/GitHub** para versionar scripts y documentación.

## Estructura del repositorio

```text
Osiptel_Proyecto/
├── scripts/
│   ├── 01_extraer_pdfs.py
│   ├── 02_ocr_pdfs.py
│   ├── 03_extraer_indicadores.py
│   ├── 04_ingesta_hdfs.py
│   ├── 05_limpieza_spark.py
│   └── 06_integracion_spark.py
├── notebooks/
│   └── 01_exploracion_fuentes.ipynb
├── data/                 # Fuentes y derivados locales
│   ├── raw/
│   └── extracted/
├── logs/                 # Auditoría y registros
├── run_pipeline.sh       # Orquestador de las seis etapas
├── requirements.txt      # Dependencias Python
└── README.md
```

Los archivos de datos grandes, los PDF, los Excel y los logs no se incluyen en GitHub. Cada integrante debe obtener y colocar las mismas fuentes para reproducir los resultados

## Ejecución del pipeline

### 0. Clonar el repositorio

```bash
git clone https://github.com/NevadoLG/ProyectOsiptel.git
cd ProyectOsiptel
```

### 1. Preparar Python y las dependencias

Desde la raíz del repositorio:

```bash
# Crear el entorno virtual una sola vez, en el directorio padre.
python3 -m venv ../bigdata_env
source ../bigdata_env/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Si el entorno bigdata_env ya existe, no hace falta recrearlo solo hay que activarlo con `source ../bigdata_env/bin/activate` y ejecuta solo la instalación de dependencias.

Comprobación:

```bash
python -c 'import pandas, openpyxl, pyspark; print("Dependencias Python: OK")'
spark-submit --version
hdfs version
```

**Herramientas del sistema (Debian):** Java 17, Hadoop/HDFS, YARN y Spark deben estar instalados y configurados. Para la extracción de PDF y OCR:

```bash
sudo apt update
sudo apt install -y poppler-utils tesseract-ocr tesseract-ocr-spa
```

### 2. Incorporar las fuentes originales

Antes de ejecutar, colocar los archivos de origen con esta estructura:

```text
data/raw/
├── excel/
│   └── 2.1 CONEXIONES DE INTERNET FIJO.xlsx
├── pdf/
│   ├── inei_tic/             # Boletines TIC de INEI
│   ├── osiptel_reportes/    # Reportes estadísticos OSIPTEL
│   └── *.pdf                # Otros PDF originales OSIPTEL
└── zip/                    # ZIP de respaldo, si están disponibles
```

### 3. Iniciar o comprobar los servicios de Big Data

En la VM con Hadoop ya configurado, si los servicios aún no están levantados:

```bash
start-dfs.sh
start-yarn.sh
```

Comprobar que NameNode, DataNode, ResourceManager y NodeManager estén disponibles:

```bash
jps
hdfs dfs -ls /
```

### 4. Ejecutar el orquestador general

Desde la raíz del repositorio, con el entorno virtual activo:

```bash
chmod +x run_pipeline.sh   # Solo la primera vez
./run_pipeline.sh
```

El script ejecuta en orden:

| Etapa | Script | Operación |
| --- | --- | --- |
| 01 | `01_extraer_pdfs.py` | Extrae texto de los PDF y genera inventario. |
| 02 | `02_ocr_pdfs.py` | Aplica OCR a documentos sin texto recuperable. |
| 03 | `03_extraer_indicadores.py` | Extrae tablas de indicadores de INEI a CSV. |
| 04 | `04_ingesta_hdfs.py` | Sube originales y derivados a HDFS; evita sobrescrituras y registra conflictos. |
| 05 | `05_limpieza_spark.py` | Normaliza y valida; deduplica filas idénticas; separa claves ambiguas; escribe Silver. |
| 06 | `06_integracion_spark.py` | Agrega cierres trimestrales y realiza JOIN OSIPTEL–INEI para Gold. |

Si alguna etapa devuelve error, el orquestador se detiene; consultar el registro en `logs/pipeline_*.log`. El orquestador verifica también la coincidencia SHA-256 del CSV de INEI entre local y HDFS.

### 5. Comprobar las salidas en HDFS

```bash
hdfs dfs -ls /datalake/osiptel_proyecto
hdfs dfs -ls /datalake/osiptel_proyecto/silver
hdfs dfs -ls /datalake/osiptel_proyecto/gold
hdfs dfs -ls /datalake/osiptel_proyecto/gold/conexiones_inei_integradas
```

Estructura principal del Data Lake:

```text
/datalake/osiptel_proyecto/
├── raw/                 # Originales preservados
├── staging/             # Texto y CSV extraídos
├── silver/
│   ├── conexiones_osiptel/               # Original normalizado
│   ├── conexiones_osiptel_depuradas/     # Filas válidas de clave única
│   ├── conexiones_osiptel_en_revision/   # Claves ambiguas
│   ├── conexiones_osiptel_invalidos/     # Registros inválidos
│   ├── conexiones_osiptel_duplicados_exactos/  # Auditoría
│   └── inei_indicadores_versiones/       # Publicaciones INEI conservadas
└── gold/
    ├── conexiones_inei_integradas/      # Parquet analítico
    └── periodos_en_revision/            # Trimestres no publicados en Gold
```

## Trabajo pendiente

- Documentar el problema de negocio y objetivos SMART.
- Completar el EDA sobre Silver depurado, gráficos e interpretaciones.
- Definir y evaluar el modelo base, el modelo analítico final y un tablero de indicadores.
- Medir tiempos, escalabilidad, costos y documentar gobierno, seguridad y ética.
- Completar el informe y anexos conforme a la guía del curso.