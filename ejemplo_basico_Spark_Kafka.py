from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Kafka-Spark Streaming") \
    .getOrCreate()

# Conectar Spark a Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load()

# Definir el esquema de los datos que Kafka envía (en este caso, texto plano)
schema = StructType().add("value", StringType())

# Convertir los datos de Kafka a un formato legible
df2 = df.selectExpr("CAST(value AS STRING)")

# Procesar los datos (en este caso, simplemente mostrar el texto)
query = df2.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Esperar a que finalice el stream
query.awaitTermination()

