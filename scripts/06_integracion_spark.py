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
        salida_gold = f"{HDFS_BASE}/gold/conexiones_inei_integradas"
        salida_revision = f"{HDFS_BASE}/gold/periodos_en_revision"
        gold.write.mode("overwrite").parquet(salida_gold)
        en_revision.write.mode("overwrite").parquet(salida_revision)
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
        print(f"Reporte: {log}")
    finally:
        spark.stop()
if __name__ == "__main__":
    main()