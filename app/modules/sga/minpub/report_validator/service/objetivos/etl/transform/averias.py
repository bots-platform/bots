from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, to_timestamp

def get_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession with optimized configurations.
    """
    return (SparkSession.builder
            .appName("AveriasProcessor")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate())

def create_empty_schema() -> StructType:
    """
    Creates the schema for an empty averias DataFrame.
    """
    return StructType([
        StructField("nro_incidencia", StringType(), True),
        StructField("Tiempo Total (HH:MM)", StringType(), True),
        StructField("Fecha y Hora Inicio", TimestampType(), True),
        StructField("Fecha y Hora Fin", TimestampType(), True)
    ])

def preprocess_df_word_averias(df: Optional[DataFrame] = None) -> DataFrame:
    """
    Preprocesses the DataFrame for averias using PySpark.
    
    Args:
        df (Optional[DataFrame]): Input DataFrame to preprocess
        
    Returns:
        DataFrame: Preprocessed DataFrame with standardized columns and data types
    """
    spark = get_spark_session()
    
    try:
        if df is None:
            # Create empty DataFrame with the defined schema
            return spark.createDataFrame([], schema=create_empty_schema())
        
        # Rename columns and cast types
        df = (df
              .withColumnRenamed("Número de ticket", "nro_incidencia")
              .withColumnRenamed("Tiempo\nTotal (HH:MM)", "Tiempo Total (HH:MM)")
              .withColumn("nro_incidencia", col("nro_incidencia").cast("string"))
              .withColumn("Fecha y Hora Inicio", 
                         to_timestamp(col("Fecha y Hora Inicio"), "dd/MM/yyyy HH:mm"))
              .withColumn("Fecha y Hora Fin", 
                         to_timestamp(col("Fecha y Hora Fin"), "dd/MM/yyyy HH:mm")))
        
        # Cache the DataFrame for better performance if it will be used multiple times
        df.cache()
        
        return df
        
    except Exception as e:
        spark.stop()
        raise Exception(f"Error preprocessing averias DataFrame: {str(e)}")
    finally:
        spark.stop()

