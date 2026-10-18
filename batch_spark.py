"""
Tarea 3 - Big Data (UNAD)
Procesamiento en batch con Apache Spark (PySpark)
Dataset: Combined Cycle Power Plant (Folds5x2_pp)

Columnas:
    AT = temperatura ambiente (°C)
    V  = vacío de escape (cm Hg)
    AP = presión ambiente (mbar)
    RH = humedad relativa (%)
    PE = energía eléctrica producida (MW)  <- variable de interés

Ejecución:
    python convertir_a_csv.py   (una sola vez, genera datos/dataset.csv)
    python batch_spark.py
"""

import os
import sys

# Spark debe usar el mismo Python del entorno virtual (evita el error
# "Python worker failed to connect back" en Windows)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import matplotlib
matplotlib.use("Agg")  # guarda las gráficas en archivos, sin abrir ventanas
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

RUTA_CSV = "datos/dataset.csv"
SALIDA = "resultados"
os.makedirs(SALIDA, exist_ok=True)

spark = SparkSession.builder.appName("Tarea3_Batch").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ------------------------------------------------------------------
# 1. CARGA
# ------------------------------------------------------------------
df = (spark.read.option("header", True).option("inferSchema", True).csv(RUTA_CSV))
print("=== ESQUEMA ===")
df.printSchema()
total_inicial = df.count()
print("Filas iniciales:", total_inicial)

# ------------------------------------------------------------------
# 2. LIMPIEZA
# ------------------------------------------------------------------
print("\n=== NULOS POR COLUMNA ===")
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

df = df.dropDuplicates().dropna()
print("Filas tras quitar duplicados y nulos:", df.count(),
      "(eliminadas:", total_inicial - df.count(), ")")

# La humedad relativa no puede superar 100 %
print("Filas con RH > 100:", df.filter(F.col("RH") > 100).count())
df = df.withColumn("RH", F.least(F.col("RH"), F.lit(100.0)))

# ------------------------------------------------------------------
# 3. TRANSFORMACIÓN
# ------------------------------------------------------------------
df = df.withColumn(
    "rango_temp",
    F.when(F.col("AT") < 10, "<10")
     .when(F.col("AT") < 20, "10-20")
     .when(F.col("AT") < 30, "20-30")
     .otherwise(">=30"))
df.cache()

# ------------------------------------------------------------------
# 4. OPERACIONES CON RDD (transformaciones y acciones)
# ------------------------------------------------------------------
print("\n=== OPERACIONES CON RDD ===")
rdd = df.select("rango_temp", "PE").rdd.map(lambda fila: (fila["rango_temp"], fila["PE"]))

# filter + count: registros con producción alta
alta = rdd.filter(lambda kv: kv[1] > 480)
print("Registros con PE > 480 MW:", alta.count())

# map + reduceByKey: promedio de PE por rango (suma y conteo por clave)
promedio_rdd = (rdd.map(lambda kv: (kv[0], (kv[1], 1)))
                   .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                   .mapValues(lambda s: round(s[0] / s[1], 2)))
print("Promedio de PE por rango (RDD):", sorted(promedio_rdd.collect()))

# take y reduce
print("Primeros 3 registros:", rdd.take(3))
print("PE máxima (reduce):", rdd.map(lambda kv: kv[1]).reduce(max))

# ------------------------------------------------------------------
# 5. ANÁLISIS EXPLORATORIO (DataFrames)
# ------------------------------------------------------------------
print("\n=== ESTADÍSTICAS DESCRIPTIVAS ===")
df.select("AT", "V", "AP", "RH", "PE").describe().show()

print("=== CORRELACIÓN CON PE ===")
corr = {c: round(df.stat.corr(c, "PE"), 3) for c in ["AT", "V", "AP", "RH"]}
print(corr)

print("\n=== ENERGÍA PROMEDIO POR RANGO DE TEMPERATURA ===")
por_rango = (df.groupBy("rango_temp")
               .agg(F.count("*").alias("registros"),
                    F.round(F.avg("PE"), 2).alias("PE_promedio"))
               .orderBy("PE_promedio"))
por_rango.show()

# ------------------------------------------------------------------
# 6. VISUALIZACIÓN
# ------------------------------------------------------------------
plt.figure(figsize=(6, 4))
plt.bar(list(corr.keys()), list(corr.values()), color="steelblue")
plt.axhline(0, color="black", linewidth=0.8)
plt.title("Correlación de cada variable con PE")
plt.tight_layout()
plt.savefig(f"{SALIDA}/correlaciones.png", dpi=150)
plt.close()

p = por_rango.toPandas().set_index("rango_temp").reindex(["<10", "10-20", "20-30", ">=30"])
plt.figure(figsize=(6, 4))
plt.bar(p.index, p["PE_promedio"], color="darkorange")
plt.title("Energía promedio (PE) por rango de temperatura")
plt.xlabel("Temperatura ambiente (AT, °C)")
plt.ylabel("PE (MW)")
plt.tight_layout()
plt.savefig(f"{SALIDA}/pe_por_rango.png", dpi=150)
plt.close()

m = df.select("AT", "PE").sample(0.3, seed=42).toPandas()
plt.figure(figsize=(6, 4))
plt.scatter(m["AT"], m["PE"], s=5, alpha=0.4)
plt.title("Temperatura ambiente vs energía producida")
plt.xlabel("AT (°C)")
plt.ylabel("PE (MW)")
plt.tight_layout()
plt.savefig(f"{SALIDA}/dispersion_at_pe.png", dpi=150)
plt.close()

por_rango.toPandas().to_csv(f"{SALIDA}/pe_por_rango.csv", index=False)
print(f"\nListo. Gráficas y tablas guardadas en '{SALIDA}/'")
spark.stop()