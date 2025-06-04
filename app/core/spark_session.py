from pyspark.sql import SparkSession
from typing import Optional
import atexit

class SparkSessionManager:
    _instance: Optional[SparkSession] = None

    @classmethod
    def get_spark_session(cls) -> SparkSession:
        if cls._instance is None:
            cls._instance = (
                SparkSession.builder
                .appName("FastAPI-Spark-App")
                .master("local[*]")  # Use all available cores
                .config("spark.driver.memory", "4g")  # Adjust based on your laptop's RAM
                .config("spark.executor.memory", "4g")
                .config("spark.sql.shuffle.partitions", "4")  # Adjust based on your data size
                .config("spark.default.parallelism", "4")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")  # Enable Arrow for better performance
                .getOrCreate()
            )
            
            # Register cleanup function
            atexit.register(cls.cleanup)
            
        return cls._instance

    @classmethod
    def cleanup(cls):
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None

# Create a global spark session instance
spark = SparkSessionManager.get_spark_session() 