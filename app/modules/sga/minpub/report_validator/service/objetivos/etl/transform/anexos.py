from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, when

from app.modules.sga.minpub.report_validator.service.objetivos.utils.spark_manager import spark_manager

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
    with spark_manager.get_session_context() as spark:
        try:
            if df is None:
                return spark.createDataFrame([], schema=create_empty_schema())
            
            df = (df
                  .withColumnRenamed("ticket", "nro_incidencia")
                  .withColumn("nro_incidencia", col("nro_incidencia").cast("string")))
            
            df.cache()
            
            return df
            
        except Exception as e:
            raise Exception(f"Error preprocessing anexos indisponibilidad DataFrame: {str(e)}")
        finally:
            if df is not None and df.is_cached:
                df.unpersist()



