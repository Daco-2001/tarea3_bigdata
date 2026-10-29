from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, stddev, expr

# 1. Inicializar la sesión de Spark para procesamiento Batch
spark = SparkSession.builder \
    .appName("BatchAnalysisVolatilidadSIPSA") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar el conjunto de datos seleccionado desde la fuente original (CSV estático)
csv_path = "Insumos_agricolas_BD_SIPSA_I_2026.csv"

print(f"Cargando dataset batch desde: {csv_path}...")
df = spark.read \
    .option("delimiter", ";") \
    .option("header", "true") \
    .option("encoding", "latin1") \
    .csv(csv_path)

# Limpiar espacios en blanco ocultos en los nombres de las columnas del CSV
df = df.toDF(*[c.strip() for c in df.columns])

# 3. Operaciones de limpieza, transformación y EDA
# Limpiamos los puntos de miles en la cadena del precio y lo convertimos a formato numérico (Double).
# Filtramos filas con valores nulos críticos para asegurar la calidad de los datos.
df_cleaned = df.withColumn(
    "PRECIO_NUM", 
    expr("cast(replace(PRECIO, '.', '') as double)")
).na.drop(subset=["PRECIO_NUM", "ARTÍCULO", "CÓDIGO DIVIPOLA"])

print(f"Registros totales válidos procesados en Batch: {df_cleaned.count()}")

# 4. Análisis analítico para responder al problema: 
# Cálculo de métricas de riesgo financiero (volatilidad, brecha y dispersión regional por DIVIPOLA)
analisis_volatilidad = df_cleaned.groupBy("ARTÍCULO", "CÓDIGO DIVIPOLA") \
    .agg(
        min("PRECIO_NUM").alias("precio_minimo"),
        max("PRECIO_NUM").alias("precio_maximo"),
        avg("PRECIO_NUM").alias("precio_promedio"),
        stddev("PRECIO_NUM").alias("volatilidad_stddev")
    ) \
    .withColumn("brecha_precio", col("precio_maximo") - col("precio_minimo")) \
    .na.fill(0, subset=["volatilidad_stddev"]) \
    .orderBy(col("volatilidad_stddev").desc())

# 5. Visualización de los resultados en consola (Evidencia del EDA y del análisis de riesgo)
print("\n" + "="*80)
print("--- TOP INSUMOS CON MAYOR VOLATILIDAD Y RIESGO FINANCIERO (BATCH) ---")
print("="*80)
analisis_volatilidad.show(20, truncate=False)

print("\n" + "="*80)
print("--- RESUMEN ESTADÍSTICO GLOBAL DEL PRECIO DE INSUMOS ---")
print("="*80)
df_cleaned.select("PRECIO_NUM").describe().show()
