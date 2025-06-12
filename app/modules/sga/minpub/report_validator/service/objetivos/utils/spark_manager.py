from contextlib import contextmanager
from typing import Generator
from pyspark.sql import SparkSession

class SparkManager:
    _instance = None
    _spark = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SparkManager, cls).__new__(cls)
        return cls._instance

    def get_session(self) -> SparkSession:
        """
        Gets or creates a SparkSession with optimized configurations.
        """
        if self._spark is None:
            self._spark = (SparkSession.builder
                .appName("AveriasProcessor")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.shuffle.partitions", "200")
                .getOrCreate())
        return self._spark

    @contextmanager
    def get_session_context(self) -> Generator[SparkSession, None, None]:
        """
        Context manager for Spark session handling.
        Ensures proper cleanup of resources.
        """
        try:
            yield self.get_session()
        finally:
            if self._spark is not None:
                self._spark.stop()
                self._spark = None

# Create a singleton instance
spark_manager = SparkManager() 