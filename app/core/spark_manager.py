from pyspark.sql import SparkSession
from typing import Optional, Generator, TYPE_CHECKING
from contextlib import contextmanager
import logging
import os

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from typing import TypeVar
    T = TypeVar('T', bound='SparkManager')

class SparkManager:
    _instance: Optional['SparkManager'] = None
    _spark: Optional[SparkSession] = None

    def __new__(cls: type['SparkManager']) -> 'SparkManager':
        if cls._instance is None:
            cls._instance = super(SparkManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.initialized = True
            self._spark = None

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

    def stop_session(self):
        """
        Stops the current Spark session if it exists.
        """
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

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