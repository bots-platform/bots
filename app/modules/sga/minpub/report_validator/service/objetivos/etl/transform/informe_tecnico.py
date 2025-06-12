from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, when, trim, to_timestamp

from app.modules.sga.minpub.report_validator.service.objetivos.utils.spark_manager import spark_manager

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
    with spark_manager.get_session_context() as spark:
        try:
            if df is None:
                return spark.createDataFrame([], schema=create_empty_schema())
            
            df = df.withColumnRenamed("ticket", "nro_incidencia")
            
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
            
            df.cache()
            
            return df
            
        except Exception as e:
            raise Exception(f"Error preprocessing Informe Tecnico DataFrame: {str(e)}")
        finally:
            if df is not None and df.is_cached:
                df.unpersist()




