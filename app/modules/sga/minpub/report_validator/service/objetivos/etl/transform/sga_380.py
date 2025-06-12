from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from app.modules.sga.minpub.report_validator.service.objetivos.utils.cleaning import (
    handle_null_values_spark
)
from app.modules.sga.minpub.report_validator.service.objetivos.utils.spark_manager import spark_manager

def preprocess_380(df: Optional[DataFrame] = None) -> DataFrame:
    """
    Preprocesses the DataFrame for SGA-380 data using PySpark.
    
    Args:
        df (Optional[DataFrame]): Input DataFrame to preprocess
        
    Returns:
        DataFrame: Preprocessed DataFrame with standardized columns and data types
    """
    with spark_manager.get_session_context() as spark:
        try:
            if df is None:
                return spark.createDataFrame([], schema=[])
            
            df = df.withColumn('startdate', F.to_timestamp('startdate', 'dd/MM/yyyy'))
            df = df.withColumn('enddate', F.to_timestamp('enddate', 'dd/MM/yyyy'))
            
            df = handle_null_values_spark(df)
            
            df.cache()
            
            return df
            
        except Exception as e:
            raise Exception(f"Error preprocessing SGA-380 DataFrame: {str(e)}")
        finally:
            if df is not None and df.is_cached:
                df.unpersist()

