from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, when, trim, to_timestamp

def get_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession with optimized configurations.
    """
    return (SparkSession.builder
            .appName("InformeTecnicoProcessor")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate())

def create_empty_schema() -> StructType:
    """
    Creates the schema for an empty Informe Tecnico DataFrame.
    """
    return StructType([
        StructField("nro_incidencia", StringType(), True),
        StructField("Fecha y Hora Inicio", TimestampType(), True),
        StructField("Fecha y Hora Fin", TimestampType(), True)
    ])

def preprocess_df_word_informe_tecnico(df: Optional[DataFrame] = None) -> DataFrame:
    """
    Preprocesses the DataFrame for Informe Tecnico data using PySpark.
    
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
        
        # Rename ticket column to nro_incidencia
        df = df.withColumnRenamed("ticket", "nro_incidencia")
        
        # Process each column with proper null handling and data type conversion
        df = df.withColumn(
            "nro_incidencia",
            when(col("nro_incidencia").isNull(), "")
            .otherwise(col("nro_incidencia").cast("string"))
        ).withColumn(
            "Fecha y Hora Inicio",
            when(col("Fecha y Hora Inicio").isNull(), None)
            .otherwise(to_timestamp(col("Fecha y Hora Inicio")))
        ).withColumn(
            "Fecha y Hora Fin",
            when(col("Fecha y Hora Fin").isNull(), None)
            .otherwise(to_timestamp(col("Fecha y Hora Fin")))
        )
        
        # Cache the DataFrame for better performance if it will be used multiple times
        df.cache()
        
        return df
        
    except Exception as e:
        spark.stop()
        raise Exception(f"Error preprocessing Informe Tecnico DataFrame: {str(e)}")
    finally:
        spark.stop()


