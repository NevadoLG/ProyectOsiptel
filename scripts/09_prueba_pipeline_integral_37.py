import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path
from time import perf_counter

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window, functions as F

PROYECTO = Path(__file__).resolve().parent.parent
RAIZ_HDFS = "hdfs:///datalake/osiptel_proyecto"
RAIZ_PRUEBAS = f"{RAIZ_HDFS}/pruebas_pipeline_37"
INEI_CSV = f"{RAIZ_HDFS}/staging/tablas/inei_indicadores_bruto.csv"
FUENTE_INEI = "boletin-tic-iit2026.pdf"
XLSX = PROYECTO / "data/raw/excel/2.1 CONEXIONES DE INTERNET FIJO.xlsx"

COLUMNAS_EXCEL = [
    "Mes", "Empresa", "Tecnologia", "Segmento", "Departamento",
    "Rango Velocidad de Bajada (nominal)", "Conexiones",
]
CLAVE = ["mes", "empresa", "tecnologia", "segmento", "departamento", "rango_velocidad"]
INDICADORES = [
    "telefonia_fija_pct", "telefonia_movil_pct", "tv_cable_pct",
    "computadora_pct", "internet_pct",
]
COLUMNAS_INEI = ["fuente_pdf", "pagina", "tipo_periodo", "anio", "trimestre", *INDICADORES]


def limpiar_texto(nombre):
    texto = F.trim(F.col(nombre))
    return F.when(F.length(texto) == 0, F.lit(None)).otherwise(texto)


def cierres(df):
    return df.filter(
        (F.col("anio") >= 2019)
        & F.col("mes").isNotNull()
        & (F.month("mes") == F.col("trimestre") * F.lit(3))
    )


def comprobar_inei(df):
    condicion = (
        F.col("fuente_pdf").isNull()
        | F.col("pagina").isNull()
        | ~F.col("tipo_periodo").isin("anual", "trimestral")
        | F.col("anio").isNull()
        | (
            (F.col("tipo_periodo") == "trimestral")
            & (F.col("trimestre").isNull() | ~F.col("trimestre").between(1, 4))
        )
    )
    for nombre in INDICADORES:
        condicion = condicion | F.col(nombre).isNull() | ~F.col(nombre).between(0, 100)
    if df.filter(condicion).limit(1).count():
        raise ValueError("Existen indicadores INEI inválidos")


def procesar_escala(spark, bruto_osiptel, bruto_inei, referencia, version, escala):
    prefijo = f"{RAIZ_PRUEBAS}/escala_{escala}x"
    print(f"\n========== PIPELINE INTEGRAL {escala}x ==========", flush=True)
    replicas = F.broadcast(
        spark.range(escala).select(F.col("id").cast("int").alias("replica_id"))
    )

    t0 = perf_counter()
    conexiones = (
        bruto_osiptel.crossJoin(replicas)
        .select(
            F.to_date(F.col("Mes"), "yyyy-MM-dd").alias("mes"),
            limpiar_texto("Empresa").alias("empresa"),
            limpiar_texto("Tecnologia").alias("tecnologia"),
            limpiar_texto("Segmento").alias("segmento"),
            limpiar_texto("Departamento").alias("departamento"),
            limpiar_texto("Rango Velocidad de Bajada (nominal)").alias("rango_velocidad"),
            F.col("Conexiones").cast("long").alias("conexiones"),
            F.col("replica_id"),
        )
        .withColumn("anio", F.year("mes"))
        .withColumn("trimestre", F.quarter("mes"))
        .withColumn("fuente_excel", F.lit(XLSX.name))
        .withColumn("version_excel", F.lit(version))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    try:
        n_original = conexiones.count()
        condicion_invalido = (
            F.col("mes").isNull()
            | F.col("conexiones").isNull()
            | (F.col("conexiones") < 0)
        )
        invalidos = conexiones.filter(condicion_invalido).withColumn(
            "motivo_revision",
            F.concat_ws("; ",
                F.when(F.col("mes").isNull(), "FECHA_INVALIDA"),
                F.when(F.col("conexiones").isNull(), "CONEXIONES_NO_NUMERICAS"),
                F.when(F.col("conexiones") < 0, "CONEXIONES_NEGATIVAS"),
            ),
        )
        n_invalidos = invalidos.count()

        validos = conexiones.filter(~condicion_invalido).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            n_validos = validos.count()
            p50, p99, p999 = validos.approxQuantile(
                "conexiones", [0.50, 0.99, 0.999], 0.0001
            )
            n_extremos = validos.filter(F.col("conexiones") > p999).count()
            n_ceros = validos.filter(F.col("conexiones") == 0).count()
            columnas_duplicado = [*CLAVE, "conexiones", "replica_id"]
            duplicados = (
                validos.groupBy(*columnas_duplicado)
                .agg(F.count(F.lit(1)).alias("apariciones"))
                .filter(F.col("apariciones") > 1)
                .withColumn("copias_omitidas", F.col("apariciones") - 1)
                .withColumn("motivo_revision", F.lit("DUPLICADO_EXACTO"))
            )
            sin_duplicados = validos.dropDuplicates(columnas_duplicado)
            n_sin_duplicados = sin_duplicados.count()
            n_duplicados = n_validos - n_sin_duplicados
            n_auditoria = duplicados.agg(
                F.coalesce(F.sum("copias_omitidas"), F.lit(0)).alias("n")
            ).first()["n"]
            assert n_duplicados == n_auditoria, "No cuadran los duplicados"

            con_estado = (
                sin_duplicados.withColumn(
                    "filas_por_clave",
                    F.count(F.lit(1)).over(Window.partitionBy(*CLAVE, "replica_id"))
                ).persist(StorageLevel.MEMORY_AND_DISK)
            )
            try:
                depurados = con_estado.filter(F.col("filas_por_clave") == 1).drop("filas_por_clave")
                revision = (
                    con_estado.filter(F.col("filas_por_clave") > 1)
                    .withColumn("motivo_revision", F.lit("CLAVE_REPETIDA_CON_CONEXIONES_DISTINTAS"))
                )
                n_depurados = depurados.count()
                n_revision = revision.count()
                if n_original != n_invalidos + n_duplicados + n_depurados + n_revision:
                    raise AssertionError("No cuadra la partición de registros Silver")

                inei = bruto_inei.select(
                    limpiar_texto("fuente_pdf").alias("fuente_pdf"),
                    F.col("pagina").cast("integer").alias("pagina"),
                    limpiar_texto("tipo_periodo").alias("tipo_periodo"),
                    F.col("anio").cast("integer").alias("anio"),
                    F.col("trimestre").cast("integer").alias("trimestre"),
                    *(F.col(c).cast("double").alias(c) for c in INDICADORES),
                )
                comprobar_inei(inei)

                salidas_silver = {
                    "conexiones_osiptel": conexiones,
                    "conexiones_osiptel_depuradas": depurados,
                    "conexiones_osiptel_en_revision": revision,
                    "conexiones_osiptel_invalidos": invalidos,
                    "conexiones_osiptel_duplicados_exactos": duplicados,
                    "inei_indicadores_versiones": inei,
                }
                for nombre, dataframe in salidas_silver.items():
                    dataframe.write.mode("overwrite").parquet(f"{prefijo}/silver/{nombre}")
            finally:
                con_estado.unpersist()
        finally:
            validos.unpersist()
    finally:
        conexiones.unpersist()

    tiempo_silver = perf_counter() - t0
    print(f"Silver {escala}x: {tiempo_silver:.3f} s; originales={n_original:,}; "
          f"depurados={n_depurados:,}; revisión={n_revision:,}", flush=True)
    t1 = perf_counter()
    depurados = spark.read.parquet(f"{prefijo}/silver/conexiones_osiptel_depuradas")
    revision = spark.read.parquet(f"{prefijo}/silver/conexiones_osiptel_en_revision")
    invalidos = spark.read.parquet(f"{prefijo}/silver/conexiones_osiptel_invalidos")
    inei = spark.read.parquet(f"{prefijo}/silver/inei_indicadores_versiones")
    if depurados.groupBy(*CLAVE, "replica_id").count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("Silver contiene claves repetidas dentro de una réplica")
    if invalidos.filter(F.col("mes").isNull()).limit(1).count():
        raise ValueError("Hay fechas inválidas no asignables a trimestre")

    cierre = cierres(depurados)
    cierres_ambiguos = cierres(revision)
    cierres_invalidos = cierres(invalidos)

    n_claves = (
        cierres_ambiguos.select("anio", "trimestre", "replica_id", *CLAVE)
        .distinct().groupBy("anio", "trimestre")
        .agg(F.count(F.lit(1)).alias("claves_repetidas"))
    )
    n_filas_revision = cierres_ambiguos.groupBy("anio", "trimestre").agg(
        F.count(F.lit(1)).alias("filas_claves_ambiguas")
    )
    alertas_ambiguas = n_filas_revision.join(n_claves, ["anio", "trimestre"])
    alertas_invalidos = cierres_invalidos.groupBy("anio", "trimestre").agg(
        F.count(F.lit(1)).alias("filas_invalidas")
    )
    en_revision = (
        alertas_ambiguas.join(alertas_invalidos, ["anio", "trimestre"], "full_outer")
        .fillna(0, subset=["claves_repetidas", "filas_claves_ambiguas", "filas_invalidas"])
        .withColumn(
            "motivo",
            F.when(
                (F.col("claves_repetidas") > 0) & (F.col("filas_invalidas") > 0),
                F.lit("CLAVES_AMBIGUAS_Y_DATOS_INVALIDOS"),
            ).when(F.col("claves_repetidas") > 0, F.lit("CLAVES_AMBIGUAS"))
            .otherwise(F.lit("DATOS_INVALIDOS")),
        )
    )
    cierre_validado = cierre.join(
        en_revision.select("anio", "trimestre"), ["anio", "trimestre"], "left_anti"
    )
    if (cierre_validado.groupBy("anio", "trimestre")
            .agg(F.countDistinct("mes").alias("fechas_distintas"))
            .filter(F.col("fechas_distintas") > 1).limit(1).count()):
        raise ValueError("Existen varias fechas de cierre en un trimestre")

    totales = cierre_validado.groupBy("anio", "trimestre").agg(
        F.sum("conexiones").cast("long").alias("conexiones_fijas"),
        F.count(F.lit(1)).alias("filas_osiptel"),
    )
    inei_elegido = (
        inei.filter((F.col("fuente_pdf") == FUENTE_INEI)
                    & (F.col("tipo_periodo") == "trimestral"))
        .select("anio", "trimestre", "internet_pct", "fuente_pdf", "pagina")
    )
    if not inei_elegido.limit(1).count():
        raise ValueError("No existe el boletín INEI seleccionado")
    if inei_elegido.groupBy("anio", "trimestre").count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError("INEI tiene periodos duplicados")

    gold = (
        totales.join(inei_elegido, ["anio", "trimestre"], "inner")
        .withColumnRenamed("internet_pct", "hogares_con_internet_pct")
        .withColumnRenamed("fuente_pdf", "fuente_inei_pdf")
        .withColumnRenamed("pagina", "pagina_inei")
        .withColumn("calidad", F.lit("SIN_CLAVES_AMBIGUAS_NI_FILAS_INVALIDAS"))
        .orderBy("anio", "trimestre")
    )
    comparados = gold.select(
        "anio", "trimestre", "conexiones_fijas", "filas_osiptel", "hogares_con_internet_pct"
    ).collect()
    assert len(comparados) == len(referencia), "Gold tiene un número de periodos inesperado"
    for fila in comparados:
        clave = (fila["anio"], fila["trimestre"])
        assert clave in referencia, f"Periodo inesperado: {clave}"
        base = referencia[clave]
        assert fila["conexiones_fijas"] == base[0] * escala, f"Conexiones incorrectas: {clave}"
        assert fila["filas_osiptel"] == base[1] * escala, f"Filas incorrectas: {clave}"
        assert fila["hogares_con_internet_pct"] == base[2], f"INEI alterado: {clave}"

    gold.write.mode("overwrite").parquet(f"{prefijo}/gold/conexiones_inei_integradas")
    en_revision.write.mode("overwrite").parquet(f"{prefijo}/gold/periodos_en_revision")
    tiempo_gold = perf_counter() - t1
    print(f"Gold {escala}x: {tiempo_gold:.3f} s; periodos={len(comparados)}; "
          "validación frente al Gold real: CORRECTA", flush=True)

    return {
        "escala": escala,
        "filas_osiptel": n_original,
        "filas_depuradas": n_depurados,
        "filas_revision": n_revision,
        "duplicados_exactos_omitidos": n_duplicados,
        "registros_inei_no_replicados": inei.count(),
        "trimestres_gold": len(comparados),
        "segundos_silver": round(tiempo_silver, 3),
        "segundos_gold": round(tiempo_gold, 3),
        "segundos_total": round(tiempo_silver + tiempo_gold, 3),
        "hdfs_pruebas": prefijo,
        "validado_contra_gold_real": True,
        "perfil_valores": {"p50": p50, "p99": p99, "p999": p999,
                           "n_extremos": n_extremos, "n_ceros": n_ceros},
    }


def main():
    parser = argparse.ArgumentParser(description="Prueba aislada Spark Silver -> Gold (3.7)")
    parser.add_argument("--escalas", nargs="+", type=int, choices=[1, 10, 100],
                        default=[1, 10], help="Escalas a ejecutar (predeterminado: 1 10)")
    args = parser.parse_args()

    if not XLSX.is_file():
        raise FileNotFoundError(f"Falta el Excel original: {XLSX}")
    version = hashlib.sha256(XLSX.read_bytes()).hexdigest()[:12]
    csv_hdfs = f"{RAIZ_HDFS}/staging/tablas/conexiones_osiptel_{version}.csv"

    spark = (
        SparkSession.builder.appName("OSIPTEL-Prueba-Integral-3.7")
        .config("spark.sql.shuffle.partitions", "16").getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        bruto_osiptel = spark.read.option("header", True).option("encoding", "UTF-8").csv(csv_hdfs)
        bruto_inei = spark.read.option("header", True).csv(INEI_CSV)
        if bruto_osiptel.columns != COLUMNAS_EXCEL:
            raise ValueError(f"Columnas de OSIPTEL cambiaron: {bruto_osiptel.columns}")
        if bruto_inei.columns != COLUMNAS_INEI:
            raise ValueError(f"Columnas de INEI cambiaron: {bruto_inei.columns}")

        gold_real = spark.read.parquet(f"{RAIZ_HDFS}/gold/conexiones_inei_integradas")
        referencia = {
            (r["anio"], r["trimestre"]):
            (r["conexiones_fijas"], r["filas_osiptel"], r["hogares_con_internet_pct"])
            for r in gold_real.select("anio", "trimestre", "conexiones_fijas", "filas_osiptel",
                                      "hogares_con_internet_pct").collect()
        }

        resultados = []
        for escala in args.escalas:
            resultados.append(procesar_escala(
                spark, bruto_osiptel, bruto_inei, referencia, version, escala
            ))

        salida = PROYECTO / "logs"
        salida.mkdir(parents=True, exist_ok=True)
        archivo = salida / f"benchmark_integral_37_{datetime.now():%Y%m%d_%H%M%S}.json"
        archivo.write_text(json.dumps({
            "fecha": datetime.now().isoformat(timespec="seconds"),
            "tipo": "Prueba integral de las transformaciones Spark 05-06",
            "nota": "No escala las descargas, extracción PDF/OCR ni la ingesta 01-04",
            "origen_sintetico": "CSV original de OSIPTEL repetido, diferenciando replica_id",
            "spark_master": spark.sparkContext.master,
            "shuffle_partitions": 16,
            "resultados": resultados,
        }, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nVALIDACIÓN INTEGRAL: CORRECTA\nEvidencia: {archivo}", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()