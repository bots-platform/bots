from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import (
    col, when, trim, regexp_replace, lit
)
from app.modules.sga.minpub.report_validator.service.objetivos.utils.cleaning import (
    cut_decimal_part
)
from app.modules.sga.minpub.report_validator.service.objetivos.utils.spark_manager import spark_manager

def create_empty_schema() -> StructType:
    """
    Creates the schema for an empty CUISMP SharePoint DataFrame.
    """
    return StructType([
        StructField("cid", StringType(), True),
        StructField("Distrito Fiscal", StringType(), True),
        StructField("CUISMP", StringType(), True),
        StructField("SEDE", StringType(), True)
    ])

def preprocess_df_cid_cuismp_sharepoint(df: Optional[DataFrame] = None) -> DataFrame:
    """
    Preprocesses the DataFrame for CUISMP SharePoint data using PySpark.
    
    Args:
        df (Optional[DataFrame]): Input DataFrame to preprocess
        
    Returns:
        DataFrame: Preprocessed DataFrame with standardized columns and data types
    """
    with spark_manager.get_session_context() as spark:
        try:
            if df is None:
                return spark.createDataFrame([], schema=create_empty_schema())
            
            df = cut_decimal_part(df, 'CUISMP')
            
            df = df.withColumnRenamed("CID", "cid")
            
            df = df.withColumn(
                "cid",
                when(col("cid").isNull(), "")
                .otherwise(col("cid").cast("string"))
            ).withColumn(
                "Distrito Fiscal",
                when(col("Distrito Fiscal").isNull(), "No disponible")
                .otherwise(trim(col("Distrito Fiscal").cast("string")))
            ).withColumn(
                "CUISMP",
                when(col("CUISMP").isNull(), "No disponible")
                .otherwise(trim(col("CUISMP").cast("string")))
            ).withColumn(
                "SEDE",
                when(col("SEDE").isNull(), "No disponible")
                .otherwise(trim(col("SEDE").cast("string")))
            )
            
            df.cache()
            
            return df
            
        except Exception as e:
            raise Exception(f"Error preprocessing CUISMP SharePoint DataFrame: {str(e)}")
        finally:
            if df is not None and df.is_cached:
                df.unpersist()
