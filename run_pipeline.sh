set -Eeuo pipefail
PROYECTO="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROYECTO"
mkdir -p logs
REGISTRO="$PROYECTO/logs/pipeline_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$REGISTRO") 2>&1
fallo() {
    codigo=$?
    echo
    echo "[ERROR] El pipeline se detuvo (codigo $codigo)."
    echo "Registro: $REGISTRO"
    exit "$codigo"
}
trap fallo ERR
etapa() {
    descripcion="$1"
    shift
    echo
    echo "============================================================"
    echo "[ETAPA] $descripcion"
    echo "============================================================"
    "$@"
    echo "[OK] $descripcion"
}
for herramienta in python hdfs spark-submit pdftotext tesseract sha256sum; do
    if ! command -v "$herramienta" >/dev/null 2>&1; then
        echo "[ERROR] Falta la herramienta requerida: $herramienta"
        exit 1
    fi
done
python -c 'import pandas, openpyxl, pyspark' || {
    echo "[ERROR] Activa primero bigdata_env e instala las dependencias del proyecto."
    exit 1
}
for numero in 01_extraer_pdfs 02_ocr_pdfs 03_extraer_indicadores 04_ingesta_hdfs 05_limpieza_spark 06_integracion_spark; do
    if [[ ! -f "scripts/${numero}.py" ]]; then
        echo "[ERROR] Falta scripts/${numero}.py"
        exit 1
    fi
done
if [[ ! -s 'data/raw/excel/2.1 CONEXIONES DE INTERNET FIJO.xlsx' ]]; then
    echo '[ERROR] Falta el Excel original de OSIPTEL en data/raw/excel/.'
    exit 1
fi
if [[ ! -d 'data/raw/pdf/inei_tic' ]]; then
    echo '[ERROR] Falta la carpeta de PDF del INEI en data/raw/pdf/inei_tic/.'
    exit 1
fi
echo "Inicio del pipeline: $(date -Is)"
echo "Proyecto: $PROYECTO"
echo "Registro: $REGISTRO"
etapa '01 - Extracción de texto de PDF' python scripts/01_extraer_pdfs.py
etapa '02 - OCR de PDF pendientes' python scripts/02_ocr_pdfs.py
etapa '03 - Extracción de indicadores INEI' python scripts/03_extraer_indicadores.py
etapa '04 - Ingesta incremental en HDFS' python scripts/04_ingesta_hdfs.py
INEI_LOCAL='data/extracted/tablas/inei_indicadores_bruto.csv'
INEI_REMOTO='/datalake/osiptel_proyecto/staging/tablas/inei_indicadores_bruto.csv'
if [[ ! -s "$INEI_LOCAL" ]]; then
    echo "[ERROR] No se generó $INEI_LOCAL"
    exit 1
fi
ETIQUETA_LOCAL="$(sha256sum "$INEI_LOCAL" | awk '{print $1}')"
ETIQUETA_HDFS="$(hdfs dfs -cat "$INEI_REMOTO" | sha256sum | awk '{print $1}')"
if [[ "$ETIQUETA_LOCAL" != "$ETIQUETA_HDFS" ]]; then
    echo '[ERROR] El CSV INEI local y el de HDFS difieren.'
    echo 'La ingesta no sobrescribe archivos existentes: hay que versionar la entrada antes de continuar.'
    exit 1
fi
echo '[OK] CSV del INEI en HDFS coincide con el contenido local (SHA-256)'
etapa '05 - Validación y escritura Silver' spark-submit --master yarn --deploy-mode client scripts/05_limpieza_spark.py
etapa '06 - Integración y escritura Gold' spark-submit --master yarn --deploy-mode client scripts/06_integracion_spark.py
etapa 'Verificar Gold Parquet' hdfs dfs -test -e /datalake/osiptel_proyecto/gold/conexiones_inei_integradas/_SUCCESS
etapa 'Verificar revisión Gold Parquet' hdfs dfs -test -e /datalake/osiptel_proyecto/gold/periodos_en_revision/_SUCCESS
echo
echo "[OK] Pipeline completo: $(date -Is)"
echo "Registro: $REGISTRO"