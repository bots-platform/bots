from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Test") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Verificar la versión de Java que está usando
print(f"Java version: {spark._jvm.System.getProperty('java.version')}")
print(f"Spark version: {spark.version}")

spark.stop()