from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, to_timestamp
import pandas as pd

from app.modules.sga.minpub.report_validator.service.objetivos.utils.spark_manager import spark_manager

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

def preprocess_df_word_averias(df: Optional[DataFrame] = None) -> pd.DataFrame:
    """
    Preprocesses the DataFrame for averias using PySpark.
    
    Args:
        df (Optional[DataFrame]): Input DataFrame to preprocess
        
    Returns:
        DataFrame: Preprocessed DataFrame with standardized columns and data types
    """
    with spark_manager.get_session_context() as spark:
        try:
            if df is None:
                empty_df = spark.createDataFrame([], schema=create_empty_schema())
                return empty_df.toPandas()
            
            df = (df
                  .withColumnRenamed("Número de ticket", "nro_incidencia")
                  .withColumnRenamed("Tiempo\nTotal (HH:MM)", "Tiempo Total (HH:MM)")
                  .withColumn("nro_incidencia", col("nro_incidencia").cast("string"))
                  .withColumn("Fecha y Hora Inicio", 
                             to_timestamp(col("Fecha y Hora Inicio"), "dd/MM/yyyy HH:mm"))
                  .withColumn("Fecha y Hora Fin", 
                             to_timestamp(col("Fecha y Hora Fin"), "dd/MM/yyyy HH:mm")))
            
            df.cache()
            
            pdf = df.toPandas()
            return pdf
            
        except Exception as e:
            raise Exception(f"Error preprocessing word averias DataFrame: {str(e)}")
        finally:
            if df is not None and df.is_cached:
                df.unpersist()
