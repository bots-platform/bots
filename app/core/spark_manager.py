from pyspark.sql import SparkSession
from typing import Optional, Generator, TYPE_CHECKING
from contextlib import contextmanager
from app.core.config import settings
import logging
import os
import pandas as pd

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
                # Basic configurations
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.shuffle.partitions", "200")
                .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.4.3_0.20.4")
                # Memory configurations
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.memory.offHeap.enabled", "true")
                .config("spark.memory.offHeap.size", "2g")
                # Session state configurations
                .config("spark.sql.warehouse.dir", "spark-warehouse")
                .config("spark.sql.session.timeZone", "UTC")
                # Type conversion configurations
                .config("spark.sql.legacy.typeCoercion.datetimeToString.enabled", "true")
                .config("spark.sql.legacy.typeCoercion.stringToTimestamp.enabled", "true")
                # Disable Arrow optimization
                .config("spark.sql.execution.arrow.pyspark.enabled", "false")
                .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
                # Add Hive support
                .enableHiveSupport()
                .getOrCreate())
            
            # Set log level to WARN to reduce noise
            self._spark.sparkContext.setLogLevel("WARN")
            
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

    def convert_column_types(self, df, column_types):
        """
        Helper method to convert column types in a DataFrame.
        
        Args:
            df: Spark DataFrame
            column_types: Dictionary mapping column names to desired types
                e.g., {"CID NUEVO": "long"}
        """
        for column, type_name in column_types.items():
            if column in df.columns:
                df = df.withColumn(column, df[column].cast(type_name))
        return df

# Create a singleton instance
spark_manager = SparkManager()

# Configure Java and Hadoop paths
os.environ["JAVA_HOME"] = settings.JAVA_HOME
os.environ["HADOOP_HOME"] = settings.HADOOP_HOME

#df= pd.read_excel("file.xlsx") 