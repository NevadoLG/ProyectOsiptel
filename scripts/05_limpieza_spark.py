import hashlib
import json
import subprocess
from datetime import datetime
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, Window, functions as F
BASE = Path(__file__).resolve().parent.parent
HDFS_BASE = "/datalake/osiptel_proyecto"
XLSX = BASE / "data/raw/excel/2.1 CONEXIONES DE INTERNET FIJO.xlsx"
INEI_HDFS = HDFS_BASE + "/staging/tablas/inei_indicadores_bruto.csv"
ESPERADAS = [
    "Mes", "Empresa", "Tecnologia", "Segmento", "Departamento",
    "Rango Velocidad de Bajada (nominal)", "Conexiones",
]
CLAVE = ["mes", "empresa", "tecnologia", "segmento", "departamento", "rango_velocidad"]
COLUMNAS_OSIPTEL = CLAVE + ["conexiones"]
INDICADORES = [
    "telefonia_fija_pct", "telefonia_movil_pct", "tv_cable_pct",
    "computadora_pct", "internet_pct",
]
COLUMNAS_INEI = ["fuente_pdf", "pagina", "tipo_periodo", "anio", "trimestre", *INDICADORES]
def hdfs_uri(ruta):
    return "hdfs://" + ruta
def hdfs(*args, verificar=True):
    return subprocess.run(
        ["hdfs", "dfs", *map(str, args)],
        check=verificar, text=True, capture_output=True,
    )
def existe_hdfs(ruta):
    return hdfs("-test", "-e", ruta, verificar=False).returncode == 0
def preparar_excel():
    if not XLSX.exists():
        raise FileNotFoundError(f"Excel original no encontrado: {XLSX}")
    huella = hashlib.sha256(XLSX.read_bytes()).hexdigest()[:12]
    carpeta = BASE / "data/extracted/tablas"
    carpeta.mkdir(parents=True, exist_ok=True)
    csv_local = carpeta / f"conexiones_osiptel_{huella}.csv"
    if not csv_local.exists():
        datos = pd.read_excel(XLSX, sheet_name="Dataset", header=3, usecols="B:H")
        if list(datos.columns) != ESPERADAS:
            raise ValueError(f"Columnas inesperadas en el Excel: {list(datos.columns)}")
        datos["Mes"] = pd.to_datetime(datos["Mes"], errors="coerce").dt.strftime("%Y-%m-%d")
        datos.to_csv(csv_local, index=False, encoding="utf-8")
        print(f"Excel convertido a CSV ({len(datos)} filas): {csv_local.name}")
    else:
        print(f"CSV intermedio conservado: {csv_local.name}")
    destino = HDFS_BASE + "/staging/tablas/" + csv_local.name
    if existe_hdfs(destino):
        tam = int(hdfs("-stat", "%b", destino).stdout.strip())
        if tam != csv_local.stat().st_size:
            raise RuntimeError(f"CSV HDFS de tamano diferente: {destino}")
        print("CSV ya existe en HDFS; no se vuelve a cargar")
    else:
        hdfs("-mkdir", "-p", HDFS_BASE + "/staging/tablas")
        hdfs("-put", str(csv_local), destino)
        print(f"CSV cargado en HDFS: {destino}")
    return destino, huella
def texto_limpio(nombre):
    texto = F.trim(F.col(nombre))
    return F.when(F.length(texto) == 0, F.lit(None)).otherwise(texto)


def completitud(df, columnas, total):
    """Conteo de nulos y % no nulos para columnas criticas y descriptivas."""
    cantidades = df.agg(*[
        F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).cast("long").alias(c)
        for c in columnas
    ]).first().asDict()
    return {
        c: {
            "nulos": int(cantidades[c] or 0),
            "completitud_pct": round(100 * (total - int(cantidades[c] or 0)) / total, 2)
            if total else 0.0,
        }
        for c in columnas
    }
def main():
    ruta_csv, version = preparar_excel()
    if not existe_hdfs(INEI_HDFS):
        raise FileNotFoundError(f"INEI no encontrado en HDFS: {INEI_HDFS}")

    spark = (
        SparkSession.builder.appName("OSIPTEL-Silver-Limpieza-Calidad")
        .config("spark.sql.shuffle.partitions", "8").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = spark.read.option("header", True).option("encoding", "UTF-8").csv(hdfs_uri(ruta_csv))
        if raw.columns != ESPERADAS:
            raise ValueError(f"Cabecera OSIPTEL inesperada: {raw.columns}")
        conexiones = (
            raw.select(
                F.to_date(F.col("Mes"), "yyyy-MM-dd").alias("mes"),
                texto_limpio("Empresa").alias("empresa"),
                texto_limpio("Tecnologia").alias("tecnologia"),
                texto_limpio("Segmento").alias("segmento"),
                texto_limpio("Departamento").alias("departamento"),
                texto_limpio("Rango Velocidad de Bajada (nominal)").alias("rango_velocidad"),
                F.col("Conexiones").cast("long").alias("conexiones"),
            )
            .withColumn("anio", F.year("mes"))
            .withColumn("trimestre", F.quarter("mes"))
            .withColumn("fuente_excel", F.lit(XLSX.name))
            .withColumn("version_excel", F.lit(version))
        )
        total_osiptel = conexiones.count()
        campos_osiptel = completitud(conexiones, COLUMNAS_OSIPTEL, total_osiptel)
        invalido = (
            F.col("mes").isNull()
            | F.col("conexiones").isNull()
            | (F.col("conexiones") < 0)
        )
        invalidos = conexiones.filter(invalido).withColumn(
            "motivo_revision",
            F.concat_ws("; ",
                F.when(F.col("mes").isNull(), "FECHA_INVALIDA"),
                F.when(F.col("conexiones").isNull(), "CONEXIONES_NO_NUMERICAS"),
                F.when(F.col("conexiones") < 0, "CONEXIONES_NEGATIVAS"),
            ),
        )
        validos = conexiones.filter(~invalido)
        n_invalidos = invalidos.count()
        n_validos = validos.count()
        p50, p99, p999 = validos.approxQuantile("conexiones", [0.50, 0.99, 0.999], 0.0001)
        n_extremos = validos.filter(F.col("conexiones") > F.lit(p999)).count()
        n_ceros = validos.filter(F.col("conexiones") == 0).count()
        duplicados_grupos = (
            validos.groupBy(*COLUMNAS_OSIPTEL)
            .agg(F.count(F.lit(1)).alias("apariciones"))
            .filter(F.col("apariciones") > 1)
            .withColumn("copias_omitidas", F.col("apariciones") - F.lit(1))
            .withColumn("motivo_revision", F.lit("DUPLICADO_EXACTO"))
        )
        sin_duplicados = validos.dropDuplicates(COLUMNAS_OSIPTEL)
        n_sin_duplicados = sin_duplicados.count()
        n_duplicados = n_validos - n_sin_duplicados
        n_auditoria = duplicados_grupos.agg(
            F.coalesce(F.sum("copias_omitidas"), F.lit(0)).alias("n")
        ).first()["n"]
        if n_duplicados != n_auditoria:
            raise AssertionError("El conteo de copias duplicadas no coincide con la auditoria")
        por_clave = Window.partitionBy(*CLAVE)
        con_estado = sin_duplicados.withColumn("filas_por_clave", F.count(F.lit(1)).over(por_clave))
        depurados = con_estado.filter(F.col("filas_por_clave") == 1).drop("filas_por_clave")
        revision = (
            con_estado.filter(F.col("filas_por_clave") > 1)
            .withColumn("motivo_revision", F.lit("CLAVE_REPETIDA_CON_CONEXIONES_DISTINTAS"))
        )
        n_depurados = depurados.count()
        n_revision = revision.count()
        n_claves_revision = revision.select(*CLAVE).distinct().count()
        if n_invalidos + n_duplicados + n_depurados + n_revision != total_osiptel:
            raise AssertionError("Las filas OSIPTEL no cuadran entre originales y destinos")
        origen_inei = spark.read.option("header", True).csv(hdfs_uri(INEI_HDFS))
        if origen_inei.columns != COLUMNAS_INEI:
            raise ValueError(f"Cabecera INEI inesperada: {origen_inei.columns}")
        inei = origen_inei.select(
            texto_limpio("fuente_pdf").alias("fuente_pdf"),
            F.col("pagina").cast("integer").alias("pagina"),
            texto_limpio("tipo_periodo").alias("tipo_periodo"),
            F.col("anio").cast("integer").alias("anio"),
            F.col("trimestre").cast("integer").alias("trimestre"),
            *(F.col(c).cast("double").alias(c) for c in INDICADORES),
        )
        total_inei = inei.count()
        campos_inei = completitud(inei, COLUMNAS_INEI, total_inei)
        condicion_error = (
            F.col("fuente_pdf").isNull()
            | F.col("pagina").isNull()
            | ~F.col("tipo_periodo").isin("anual", "trimestral")
            | F.col("anio").isNull()
            | ((F.col("tipo_periodo") == "trimestral") &
               (F.col("trimestre").isNull() | ~F.col("trimestre").between(1, 4)))
        )
        for nombre in INDICADORES:
            condicion_error = condicion_error | F.col(nombre).isNull() | ~F.col(nombre).between(0, 100)
        inei_invalidos = inei.filter(condicion_error).count()
        if inei_invalidos:
            raise ValueError(f"INEI contiene {inei_invalidos} filas invalidas: revisar antes de escribir Silver")
        salidas = {
            "original_normalizado": HDFS_BASE + "/silver/conexiones_osiptel",
            "depurados": HDFS_BASE + "/silver/conexiones_osiptel_depuradas",
            "claves_en_revision": HDFS_BASE + "/silver/conexiones_osiptel_en_revision",
            "registros_invalidos": HDFS_BASE + "/silver/conexiones_osiptel_invalidos",
            "duplicados_exactos_auditoria": HDFS_BASE + "/silver/conexiones_osiptel_duplicados_exactos",
            "inei_versiones": HDFS_BASE + "/silver/inei_indicadores_versiones",
        }
        conexiones.write.mode("overwrite").parquet(hdfs_uri(salidas["original_normalizado"]))
        depurados.write.mode("overwrite").parquet(hdfs_uri(salidas["depurados"]))
        revision.write.mode("overwrite").parquet(hdfs_uri(salidas["claves_en_revision"]))
        invalidos.write.mode("overwrite").parquet(hdfs_uri(salidas["registros_invalidos"]))
        duplicados_grupos.write.mode("overwrite").parquet(hdfs_uri(salidas["duplicados_exactos_auditoria"]))
        inei.write.mode("overwrite").parquet(hdfs_uri(salidas["inei_versiones"]))
        informe = {
            "fecha_ejecucion": datetime.now().isoformat(timespec="seconds"),
            "filas_osiptel_originales": total_osiptel,
            "filas_osiptel_invalidas": n_invalidos,
            "filas_osiptel_duplicadas_exactas_omitidas": n_duplicados,
            "filas_osiptel_claves_ambiguas_en_revision": n_revision,
            "claves_osiptel_ambiguas": n_claves_revision,
            "filas_osiptel_depuradas": n_depurados,
            "cuadre_filas": {
                "originales": total_osiptel,
                "desglose": n_invalidos + n_duplicados + n_revision + n_depurados,
            },
            "completitud_osiptel": campos_osiptel,
            "perfil_conexiones": {
                "ceros_conservados": n_ceros,
                "percentil_50": p50,
                "percentil_99": p99,
                "percentil_99_9": p999,
                "filas_sobre_percentil_99_9": n_extremos,
                "criterio_atipicos": (
                    "Extremos identificados por percentil para revision descriptiva; "
                    "no se excluyen ni imputan por ser posibles valores reales"
                ),
            },
            "regla_nulos_categoricos": "Se conservan como nulos; no se imputan categorias ficticias",
            "regla_duplicados": "Una copia por fila exactamente igual; copias extra auditadas",
            "regla_claves_ambiguas": "Separar todas las filas de la clave tras la deduplicacion exacta",
            "filas_inei": total_inei,
            "filas_inei_invalidas": inei_invalidos,
            "completitud_inei": campos_inei,
            "versiones_inei_conservadas": True,
            "salidas_hdfs": salidas,
        }
        carpeta_logs = BASE / "logs"
        carpeta_logs.mkdir(parents=True, exist_ok=True)
        log = carpeta_logs / f"silver_calidad_{datetime.now():%Y%m%d_%H%M%S}.json"
        log.write_text(json.dumps(informe, ensure_ascii=False, indent=2), encoding="utf-8")
        print("\nSILVER GENERADO - LIMPIEZA Y CALIDAD")
        print(f"OSIPTEL originales: {total_osiptel}")
        print(f"Invalidos: {n_invalidos}; copias exactas omitidas: {n_duplicados}")
        print(f"Claves ambiguas: {n_claves_revision}; filas en revision: {n_revision}")
        print(f"Valores muy altos (sobre P99.9) conservados: {n_extremos}")
        print(f"OSIPTEL depurados: {n_depurados}")
        print(f"INEI versiones: {total_inei}; invalidos: {inei_invalidos}")
        print(f"Reporte: {log}")
    finally:
        spark.stop()
if __name__ == "__main__":
    main()