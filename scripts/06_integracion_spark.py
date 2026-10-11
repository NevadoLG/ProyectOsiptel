import json
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
BASE = Path(__file__).resolve().parent.parent
HDFS_BASE = "hdfs:///datalake/osiptel_proyecto"
FUENTE_INEI = "boletin-tic-iit2026.pdf"
CLAVE = ["mes", "empresa", "tecnologia", "segmento", "departamento", "rango_velocidad"]
def cierre_trimestre(df):
    return df.filter(
        (F.col("anio") >= 2019)
        & F.col("mes").isNotNull()
        & (F.month("mes") == F.col("trimestre") * F.lit(3))
    )
def main():
    spark = (
        SparkSession.builder.appName("OSIPTEL-Gold-Integracion-Depurada")
        .config("spark.sql.shuffle.partitions", "8").getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        depurados = spark.read.parquet(f"{HDFS_BASE}/silver/conexiones_osiptel_depuradas")
        revision = spark.read.parquet(f"{HDFS_BASE}/silver/conexiones_osiptel_en_revision")
        invalidos = spark.read.parquet(f"{HDFS_BASE}/silver/conexiones_osiptel_invalidos")
        inei = spark.read.parquet(f"{HDFS_BASE}/silver/inei_indicadores_versiones")
        reportes = spark.read.parquet(f"{HDFS_BASE}/silver/osiptel_reportes_indicadores")
        if depurados.groupBy(*CLAVE).count().filter(F.col("count") > 1).limit(1).count():
            raise ValueError("Silver depurado aun contiene claves repetidas")
        if invalidos.filter(F.col("mes").isNull()).limit(1).count():
            raise ValueError("OSIPTEL tiene fechas invalidas sin trimestre asignable; revisar Silver")
        cierre = cierre_trimestre(depurados)
        cierres_ambiguos = cierre_trimestre(revision)
        cierres_invalidos = cierre_trimestre(invalidos)
        n_claves = (
            cierres_ambiguos.select("anio", "trimestre", *CLAVE)
            .distinct()
            .groupBy("anio", "trimestre")
            .agg(F.count(F.lit(1)).alias("claves_repetidas"))
        )
        n_filas_revision = (
            cierres_ambiguos.groupBy("anio", "trimestre")
            .agg(F.count(F.lit(1)).alias("filas_claves_ambiguas"))
        )
        alertas_ambiguas = n_filas_revision.join(n_claves, ["anio", "trimestre"])
        alertas_invalidos = (
            cierres_invalidos.groupBy("anio", "trimestre")
            .agg(F.count(F.lit(1)).alias("filas_invalidas"))
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
        multiples_fechas = (
            cierre_validado.groupBy("anio", "trimestre")
            .agg(F.countDistinct("mes").alias("fechas_distintas"))
            .filter(F.col("fechas_distintas") > 1)
        )
        if multiples_fechas.limit(1).count():
            raise ValueError("Mas de una fecha de cierre OSIPTEL dentro de un trimestre")
        totales = (
            cierre_validado.groupBy("anio", "trimestre")
            .agg(
                F.sum("conexiones").cast("long").alias("conexiones_fijas"),
                F.count(F.lit(1)).alias("filas_osiptel"),
            )
        )
        inei_elegido = (
            inei.filter(
                (F.col("fuente_pdf") == FUENTE_INEI)
                & (F.col("tipo_periodo") == "trimestral")
            ).select("anio", "trimestre", "internet_pct", "fuente_pdf", "pagina")
        )
        if not inei_elegido.limit(1).count():
            raise ValueError(f"No existe la serie INEI seleccionada: {FUENTE_INEI}")
        if inei_elegido.groupBy("anio", "trimestre").count().filter(F.col("count") > 1).limit(1).count():
            raise ValueError("INEI contiene trimestres repetidos en la publicacion seleccionada")

        gold = (
            totales.join(inei_elegido, ["anio", "trimestre"], "inner")
            .withColumnRenamed("internet_pct", "hogares_con_internet_pct")
            .withColumnRenamed("fuente_pdf", "fuente_inei_pdf")
            .withColumnRenamed("pagina", "pagina_inei")
            .withColumn("calidad", F.lit("SIN_CLAVES_AMBIGUAS_NI_FILAS_INVALIDAS"))
            .orderBy("anio", "trimestre")
        )
        n_inei = inei_elegido.count()
        n_cierre = (
            cierre.select("anio", "trimestre")
            .unionByName(cierres_ambiguos.select("anio", "trimestre"))
            .unionByName(cierres_invalidos.select("anio", "trimestre"))
            .distinct().count()
        )
        n_revision = en_revision.count()
        n_gold = gold.count()
        if not n_gold:
            raise ValueError("El JOIN no produjo trimestres validos")
        por_periodo = cierre_validado.groupBy("anio", "trimestre").agg(
            F.sum("conexiones").cast("double").alias("total_fijo"),
            F.sum(F.when(F.lower(F.col("segmento")) == "residencial",
                         F.col("conexiones")).otherwise(0)).cast("double").alias("residencial"),
            F.sum(F.when(F.lower(F.col("segmento")) == "comercial",
                         F.col("conexiones")).otherwise(0)).cast("double").alias("comercial"),
            F.sum(F.when(F.lower(F.col("tecnologia")).contains("fibra"),
                         F.col("conexiones")).otherwise(0)).cast("double").alias("fibra"),
        ).withColumn("pct_fibra", F.when(
            F.col("total_fijo") > 0,
            F.round(F.col("fibra") / F.col("total_fijo") * 100, 2)
        ))
        definiciones = [
            ("conexiones_fijas", "total_fijo"),
            ("conexiones_residenciales", "residencial"),
            ("conexiones_comerciales", "comercial"),
            ("participacion_fibra", "pct_fibra"),
        ]
        referencias = None
        for indicador, columna in definiciones:
            parte = por_periodo.select(
                "anio", "trimestre", F.lit(indicador).alias("indicador"),
                F.col(columna).alias("valor_silver")
            )
            referencias = parte if referencias is None else referencias.unionByName(parte)
        auditoria = (
            reportes.join(referencias, ["anio", "trimestre", "indicador"], "left")
            .withColumn("diferencia", F.round(F.col("valor") - F.col("valor_silver"), 4))
            .withColumn("tolerancia", F.when(F.col("unidad") == "porcentaje", F.lit(0.011))
                        .otherwise(F.lit(0.0)))
            .withColumn("estado", F.when(F.col("valor_silver").isNull(), "SIN_REFERENCIA")
                        .when(F.abs(F.col("diferencia")) <= F.col("tolerancia"), "COINCIDE")
                        .otherwise("DIFERENCIA"))
            .orderBy("anio", "trimestre", "indicador")
        )
        n_docs = auditoria.count()
        n_coinciden = auditoria.filter(F.col("estado") == "COINCIDE").count()
        n_difieren = auditoria.filter(F.col("estado") == "DIFERENCIA").count()
        n_sin_ref = n_docs - n_coinciden - n_difieren
        if not n_docs:
            raise ValueError("No hay indicadores de PDF para contrastar")
        # Diferencias son hallazgos de calidad, NO un motivo para alterar el Excel.
        salida_gold = f"{HDFS_BASE}/gold/conexiones_inei_integradas"
        salida_revision = f"{HDFS_BASE}/gold/periodos_en_revision"
        salida_auditoria = f"{HDFS_BASE}/gold/validacion_documental_osiptel"
        gold.write.mode("overwrite").parquet(salida_gold)
        en_revision.write.mode("overwrite").parquet(salida_revision)
        auditoria.write.mode("overwrite").parquet(salida_auditoria)
        log_dir = BASE / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        informe = {
            "fecha_ejecucion": datetime.now().isoformat(timespec="seconds"),
            "fuente_inei_elegida": FUENTE_INEI,
            "trimestres_inei": n_inei,
            "trimestres_osiptel_con_cierre": n_cierre,
            "trimestres_osiptel_en_revision": n_revision,
            "trimestres_integrados_validos": n_gold,
            "regla": "Excluir trimestres con clave ambigua o registro invalido al cierre",
            "integracion": "JOIN por anio y trimestre, total OSIPTEL al cierre trimestral",
            "advertencia": "INEI mide hogares con internet en general; OSIPTEL, conexiones de internet fijo. No implica causalidad.",
            "salida_gold": salida_gold,
            "salida_revision": salida_revision,
            "indicadores_pdf_contrastados": n_docs,
            "cifras_coincidentes": n_coinciden,
            "cifras_con_diferencia": n_difieren,
            "cifras_sin_referencia": n_sin_ref,
            "salida_validacion_documental": salida_auditoria,
        }
        log = log_dir / f"gold_calidad_{datetime.now():%Y%m%d_%H%M%S}.json"
        log.write_text(json.dumps(informe, indent=2, ensure_ascii=False), encoding="utf-8")
        print("\nGOLD GENERADO - DESDE SILVER DEPURADO")
        print(f"Trimestres INEI: {n_inei}")
        print(f"Trimestres con cierre OSIPTEL: {n_cierre}")
        print(f"Trimestres OSIPTEL en revision: {n_revision}")
        print(f"Trimestres integrados validos: {n_gold}")
        print(f"Parquet: {salida_gold}")
        print(f"Revision: {salida_revision}")
        print(f"Auditoria PDF: {n_docs} indicadores; {n_coinciden} coinciden, "
              f"{n_difieren} diferencias, {n_sin_ref} sin referencia")
        print(f"Auditoria Parquet: {salida_auditoria}")
        print(f"Reporte: {log}")
    finally:
        spark.stop()
if __name__ == "__main__":
    main()