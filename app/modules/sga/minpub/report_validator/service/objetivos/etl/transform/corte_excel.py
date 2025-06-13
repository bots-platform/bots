from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import (
    col, to_timestamp, when, regexp_replace, 
    concat, lpad, format_string, datediff, 
    unix_timestamp, expr, lit
)
from app.modules.sga.minpub.report_validator.service.objetivos.utils.cleaning import (
    handle_null_values, cut_decimal_part
)
from app.modules.sga.minpub.report_validator.service.objetivos.utils.etl_utils import (
    to_hhmm,
    parse_hhmm_to_minutes,
    hhmm_to_minutes,
    extract_total_hours
)
from app.modules.sga.minpub.report_validator.service.objetivos.utils.spark_manager import spark_manager
import pandas as pd

def create_empty_schema() -> StructType:
    """
    Creates the schema for an empty corte excel DataFrame.
    """
    return StructType([
        StructField("nro_incidencia", StringType(), True),
        StructField("CUISMP", StringType(), True),
        StructField("DF", StringType(), True),
        StructField("DETERMINACIÓN DE LA CAUSA", StringType(), True),
        StructField("TIPO CASO", StringType(), True),
        StructField("CID", StringType(), True),
        StructField("MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS", StringType(), True),
        StructField("FECHA Y HORA INICIO", TimestampType(), True),
        StructField("FECHA Y HORA FIN", TimestampType(), True),
        StructField("TIEMPO (HH:MM)", StringType(), True),
        StructField("FIN-INICIO (HH:MM)", StringType(), True)
    ])

def preprocess_corte_excel(df: Optional[DataFrame] = None) -> pd.DataFrame:
    """
    Preprocesses the DataFrame for corte excel using PySpark.
    
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
            
            df = df.withColumnRenamed("TICKET", "nro_incidencia")
            
            cols_to_str = [
                'nro_incidencia',
                'CODINCIDENCEPADRE',
                'CUISMP',
                'DF',
                'DETERMINACIÓN DE LA CAUSA',
                'TIPO CASO',
                'CID',
                'MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'
            ]
            
            for col_name in cols_to_str:
                df = df.withColumn(
                    col_name,
                    when(col(col_name).isNull() | (col(col_name) == ""), "No disponible")
                    .otherwise(col(col_name).cast("string").trim())
                )
            
            df = df.withColumn(
                "FECHA Y HORA INICIO",
                to_timestamp(col("FECHA Y HORA INICIO"), "yyyy-MM-dd")
            ).withColumn(
                "FECHA Y HORA FIN",
                to_timestamp(col("FECHA Y HORA FIN"), "yyyy-MM-dd")
            )
            
            df = df.withColumn(
                "TIEMPO (HH:MM)_trimed",
                when(col("TIEMPO (HH:MM)").endsWith(":00"), 
                     expr("substring(TIEMPO (HH:MM), 1, 5)"))
                .otherwise(col("TIEMPO (HH:MM)"))
            )
            
            df = df.withColumn(
                "FIN-INICIO (HH:MM)_trimed",
                expr("to_hhmm(FIN-INICIO (HH:MM))")
            )
            
            df = df.withColumn(
                "FECHA_Y_HORA_INICIO_fmt",
                when(col("FECHA Y HORA INICIO").isNull(), "N/A")
                .otherwise(format_string("%02d/%02d/%04d %02d:%02d",
                    col("FECHA Y HORA INICIO").day,
                    col("FECHA Y HORA INICIO").month,
                    col("FECHA Y HORA INICIO").year,
                    col("FECHA Y HORA INICIO").hour,
                    col("FECHA Y HORA INICIO").minute
                ))
            ).withColumn(
                "FECHA_Y_HORA_FIN_fmt",
                when(col("FECHA Y HORA FIN").isNull(), "N/A")
                .otherwise(format_string("%02d/%02d/%04d %02d:%02d",
                    col("FECHA Y HORA FIN").day,
                    col("FECHA Y HORA FIN").month,
                    col("FECHA Y HORA FIN").year,
                    col("FECHA Y HORA FIN").hour,
                    col("FECHA Y HORA FIN").minute
                ))
            )
            
            df = df.withColumn(
                "duration_diff_corte_sec",
                unix_timestamp(col("FECHA Y HORA FIN")) - unix_timestamp(col("FECHA Y HORA INICIO"))
            ).withColumn(
                "diff_corte_sec_hhmm",
                concat(
                    lpad((col("duration_diff_corte_sec") / 3600).cast("int"), 2, "0"),
                    lit(":"),
                    lpad(((col("duration_diff_corte_sec") % 3600) / 60).cast("int"), 2, "0")
                )
            )
            
            df = df.withColumn(
                "fin_inicio_hhmm_column_corte_to_minutes",
                expr("parse_hhmm_to_minutes(FIN-INICIO (HH:MM)_trimed)")
            ).withColumn(
                "duration_diff_corte_min",
                expr("hhmm_to_minutes(diff_corte_sec_hhmm)")
            ).withColumn(
                "extracted_hour",
                expr("extract_total_hours(TIEMPO (HH:MM))").cast(IntegerType())
            )
            
            df.cache()
            
            pdf = df.toPandas()
            return pdf
            
        except Exception as e:
            raise Exception(f"Error preprocessing corte excel DataFrame: {str(e)}")
        finally:
            if df is not None and df.is_cached:
                df.unpersist()