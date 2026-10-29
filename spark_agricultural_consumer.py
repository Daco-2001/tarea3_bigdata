from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. Inicializar Spark con el conector de Kafka
spark = SparkSession.builder \
    .appName("InsumosAgricolasStreamingAnalytics") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Definir esquema del DANE
schema = StructType([
    StructField("MES_AÑO", StringType()),
    StructField("CÓDIGO DIVIPOLA", StringType()),
    StructField("CÓDIGO CPC", StringType()),
    StructField("ARTÍCULO", StringType()),
    StructField("CASA COMERCIAL", StringType()),
    StructField("REGISTRO ICA", StringType()),
    StructField("UNIDAD DE MEDIDA", StringType()),
    StructField("PRECIO", StringType())
])

# 3. Leer stream desde Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "insumos_agricolas_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Parsear JSON a DataFrame estructurado
parsed_df = df_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 5. Transformación y Análisis en Tiempo Real:
# Convertimos el precio a numérico para poder operar matemáticamente con él
cleaned_df = parsed_df.withColumn(
    "PRECIO_NUM", 
    col("PRECIO").cast(DoubleType())
)

# Agregación en streaming: conteo de registros y precio promedio por artículo
stats_df = cleaned_df.groupBy("ARTÍCULO") \
    .agg(
        count("*").alias("total_registros"),
        avg("PRECIO_NUM").alias("precio_promedio")
    )

# 6. Salida en consola usando outputMode("update") para ver las métricas en tiempo real
query = stats_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
