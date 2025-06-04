from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, when

def get_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession with optimized configurations.
    """
    return (SparkSession.builder
            .appName("AnexosProcessor")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate())

def create_empty_schema() -> StructType:
    """
    Creates the schema for an empty anexos indisponibilidad DataFrame.
    """
    return StructType([
        StructField("nro_incidencia", StringType(), True),
        StructField("indisponibilidad_header", StringType(), True),
        StructField("indisponibilidad_periodos", StringType(), True),
        StructField("indisponibilidad_footer", StringType(), True),
        StructField("indisponibilidad_total", StringType(), True)
    ])

def preprocess_df_word_anexos_indisponibilidad(df: Optional[DataFrame] = None) -> DataFrame:
    """
    Preprocesses the DataFrame for anexos indisponibilidad using PySpark.
    
    Args:
        df (Optional[DataFrame]): Input DataFrame to preprocess
        
    Returns:
        DataFrame: Preprocessed DataFrame with standardized columns
    """
    spark = get_spark_session()
    
    try:
        if df is None:
            # Create empty DataFrame with the defined schema
            return spark.createDataFrame([], schema=create_empty_schema())
        
        # Rename column and cast nro_incidencia to string
        df = (df
              .withColumnRenamed("ticket", "nro_incidencia")
              .withColumn("nro_incidencia", col("nro_incidencia").cast("string")))
        
        # Cache the DataFrame for better performance if it will be used multiple times
        df.cache()
        
        return df
        
    except Exception as e:
        spark.stop()
        raise Exception(f"Error preprocessing anexos indisponibilidad DataFrame: {str(e)}")
    finally:
        spark.stop()



