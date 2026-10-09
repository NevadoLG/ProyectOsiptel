# Proyecto Big Data - OSIPTEL

Pipeline de datos para el análisis de la brecha digital y la
infraestructura de telecomunicaciones fijas en el Perú.

## Entorno

- Despliegue: On-premise
- Sistema operativo: Debian
- Almacenamiento previsto: Hadoop HDFS
- Procesamiento previsto: Apache Spark y PySpark
- Exploración y análisis: JupyterLab

## Fuentes de datos

- Datos estructurados: conexiones de internet fijo de OSIPTEL (Excel).
- Datos no estructurados: 142 informes de OSIPTEL (PDF).

Los datasets originales no se incluyen en el repositorio.

## Avance de implementación

- Extracción de texto de 90 PDF mediante pdftotext.
- Recuperación de 52 PDF mediante Tesseract OCR.
- Generación de 142 archivos TXT.
- Registro de resultados de extracción y OCR.

## Scripts

- scripts/01_extraer_pdfs.py: extracción de texto de archivos PDF.
- scripts/02_ocr_pdfs.py: OCR de documentos sin texto extraíble.

## Estado actual

Etapa inicial de extracción completada.

Pendiente:
- Validación y mejora de la ejecución repetida de los scripts.
- Ingesta a HDFS.
- Exploración y limpieza de datos.
- Integración de fuentes mediante Spark.
- Almacenamiento en Parquet.
- Análisis y modelado.
