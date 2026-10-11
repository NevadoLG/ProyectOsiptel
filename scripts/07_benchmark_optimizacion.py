from time import perf_counter
from pathlib import Path
from datetime import datetime
import json
from pyspark.sql import SparkSession, functions as F
spark = (
    SparkSession.builder
    .appName("OSIPTEL-Prueba-Optimizacion-3.6")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
ruta = "hdfs:///datalake/osiptel_proyecto/silver/conexiones_osiptel_depuradas"
try:
    datos = spark.read.parquet(ruta).select(
        "mes", "anio", "departamento", "conexiones"
    )
    def ejecutar_consultas(df):
        resultados = []
        for repeticion in range(3):
            for anio in [2022, 2023, 2024, 2025, 2026]:
                filas = (
                    df.filter(F.col("anio") == anio)
                    .groupBy("mes", "departamento")
                    .agg(
                        F.sum("conexiones").alias("total")
                    )
                    .collect()
                )
                resultados.append((
                    repeticion,
                    anio,
                    len(filas),
                    sum(int(f["total"]) for f in filas)
                ))
        return resultados
    inicio = perf_counter()
    resultado_a = ejecutar_consultas(datos)
    tiempo_a = perf_counter() - inicio

    inicio = perf_counter()
    datos_cache = datos.cache()
    datos_cache.count()
    tiempo_preparacion = perf_counter() - inicio
    inicio = perf_counter()
    resultado_b = ejecutar_consultas(datos_cache)
    tiempo_b = perf_counter() - inicio
    assert resultado_a == resultado_b, (
        "ERROR: los resultados no coinciden"
    )
    datos_cache.unpersist()
    print("\n========== OPTIMIZACIÓN 3.6 ==========")
    print(f"Sin caché:           {tiempo_a:.3f} segundos")
    print(f"Preparación caché:   {tiempo_preparacion:.3f} segundos")
    print(f"Consultas con caché: {tiempo_b:.3f} segundos")
    print(f"Total con caché:     {tiempo_preparacion + tiempo_b:.3f} segundos")
    print("\nResultados equivalentes: SÍ")
    registro = {
        "fecha": datetime.now().isoformat(),
        "spark_master": spark.sparkContext.master,
        "optimizacion": "cache",
        "consultas_ejecutadas": len(resultado_a),
        "sin_cache_seg": round(tiempo_a, 3),
        "preparacion_cache_seg": round(tiempo_preparacion, 3),
        "consultas_con_cache_seg": round(tiempo_b, 3),
        "total_con_cache_seg": round(tiempo_preparacion + tiempo_b, 3),
        "resultados_equivalentes": True
    }
    Path("logs").mkdir(exist_ok=True)
    salida = Path("logs") / "benchmark_optimizacion_36.json"
    salida.write_text(
        json.dumps(registro, indent=2),
        encoding="utf-8"
    )
    print(f"Evidencia guardada: {salida}")
finally:
    spark.stop()