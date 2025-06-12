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

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self._spark = self._create_spark_session()
        return self._spark

    def _create_spark_session(self) -> SparkSession:
        """Creates a new SparkSession with configurations optimized for local development."""
        # Calculate memory settings based on available RAM
        # Leave 4GB for system and other applications
        available_memory = 12  # GB
        driver_memory = f"{available_memory}g"
        executor_memory = f"{available_memory}g"
        
        # Calculate number of cores (leave 2 cores for system)
        num_cores = max(1, os.cpu_count() - 2)
        
        return (SparkSession.builder
                .appName("SGA Validator")
                # Memory configurations
                .config("spark.driver.memory", driver_memory)
                .config("spark.executor.memory", executor_memory)
                .config("spark.driver.maxResultSize", f"{available_memory}g")
                
                # Core configurations
                .config("spark.executor.cores", str(num_cores))
                .config("spark.driver.cores", str(num_cores))
                
                # Performance optimizations
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                
                # Reduce shuffle partitions for local development
                .config("spark.sql.shuffle.partitions", "4")
                
                # Local mode optimizations
                .config("spark.driver.host", "localhost")
                .config("spark.driver.bindAddress", "localhost")
                .config("spark.local.dir", "/tmp/spark-temp")
                
                # Memory management
                .config("spark.memory.fraction", "0.8")
                .config("spark.memory.storageFraction", "0.3")
                
                # Garbage collection
                .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
                .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC")
                
                # Local mode
                .master("local[*]")
                .getOrCreate())

    def stop(self):
        """Stops the Spark session if it exists."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    @contextmanager
    def get_session(self) -> Generator[SparkSession, None, None]:
        """
        Gets or creates a SparkSession with optimized configurations.
        """
        try:
            yield self.spark
        except Exception as e:
            logger.error(f"Error in Spark session: {str(e)}")
            raise
        finally:
            # Don't stop the session here, let the manager handle it
            pass

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

# Global instance
spark_manager = SparkManager() 