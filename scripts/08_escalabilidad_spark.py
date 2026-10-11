from pyspark.sql import SparkSession, functions as F
from time import perf_counter
from pathlib import Path
from datetime import datetime
import json
spark = (
    SparkSession.builder
    .appName("OSIPTEL-Escalabilidad-3.7")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
base = "hdfs:///datalake/osiptel_proyecto"
ruta_silver = (
    f"{base}/silver/conexiones_osiptel_depuradas"
)
ruta_pruebas = f"{base}/pruebas_escalabilidad"
resultados = []
try:
    silver = spark.read.parquet(ruta_silver)
    for escala in [1, 10, 100]:
        print(f"\n========== ESCALA {escala}x ==========")
        ruta_salida = (
            f"{ruta_pruebas}/escala_{escala}x"
        )
        replicas = (
            spark.range(escala)
            .select(
                F.col("id")
                .cast("int")
                .alias("replica_id")
            )
        )
        inicio = perf_counter()
        datos_replicados = silver.crossJoin(replicas)
        (
            datos_replicados
            .repartition(4)
            .write
            .mode("overwrite")
            .parquet(ruta_salida)
        )
        tiempo_generacion = perf_counter() - inicio
        inicio = perf_counter()
        datos = spark.read.parquet(ruta_salida)
        cantidad = datos.count()
        resumen = (
            datos.groupBy("anio", "departamento")
            .agg(
                F.sum("conexiones").alias("total")
            )
            .collect()
        )
        tiempo_procesamiento = perf_counter() - inicio
        suma_conexiones = sum(
            int(fila["total"] or 0)
            for fila in resumen
        )
        esperados = 128095 * escala
        assert cantidad == esperados, (
            f"Se esperaban {esperados} registros, "
            f"pero se obtuvieron {cantidad}"
        )
        resultado = {
            "escala": escala,
            "registros": cantidad,
            "tiempo_generacion_seg": round(
                tiempo_generacion, 3
            ),
            "tiempo_procesamiento_seg": round(
                tiempo_procesamiento, 3
            ),
            "suma_conexiones": suma_conexiones,
            "ruta_hdfs": ruta_salida
        }
        resultados.append(resultado)
        print("Registros:", f"{cantidad:,}")
        print(
            "Generación:",
            f"{tiempo_generacion:.3f} s"
        )
        print(
            "Procesamiento:",
            f"{tiempo_procesamiento:.3f} s"
        )
        print("Validación de cantidad: CORRECTA")
    assert (
        resultados[2]["suma_conexiones"]
        == resultados[0]["suma_conexiones"] * 100
    ), "Las sumas de las réplicas no coinciden"
    print("\nVALIDACIÓN DE RESULTADOS: CORRECTA")
    Path("logs").mkdir(exist_ok=True)
    evidencia = {
        "fecha": datetime.now().isoformat(),
        "spark_master": spark.sparkContext.master,
        "resultados": resultados
    }
    archivo = Path(
        "logs/benchmark_escalabilidad_37.json"
    )
    archivo.write_text(
        json.dumps(evidencia, indent=2),
        encoding="utf-8"
    )
    print("Evidencia guardada:", archivo)
finally:
    spark.stop()