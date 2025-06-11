from typing import List, Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, when, lit, coalesce

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import log_exceptions

def get_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession with optimized configurations.
    """
    return (SparkSession.builder
            .appName("AveriasTelefoniaProcessor")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate())

@log_exceptions
def merge_word_telefonia_averias_corte_excel(
        df_word_telefonia_averias: DataFrame,
        df_corte_excel: DataFrame,
        match_type: str
    ) -> DataFrame:
        """
        Common merge function for Objective 2 using PySpark.

        Merges:
          - corte-excel with word_telefonia on 'nro_incidencia'

        Args:
            df_word_telefonia_averias: DataFrame containing telefono averias data
            df_corte_excel: DataFrame containing corte excel data
            match_type: Type of match to filter ('left_only', 'both', 'right_only')

        Returns:
            DataFrame: Filtered DataFrame with matched rows based on match_type
        """
        spark = get_spark_session()
        try:
            # Perform left join using Spark SQL
            df_merge_word_telefonia_corte_excel = (
                df_word_telefonia_averias
                .join(
                    df_corte_excel,
                    on='nro_incidencia',
                    how='left'
                )
            )

            # Cache the result for better performance if it will be used multiple times
            df_merge_word_telefonia_corte_excel.cache()

            # Filter based on match type
            # In Spark, we need to handle the match type differently since there's no direct indicator
            if match_type == 'left_only':
                # Rows that exist only in the left DataFrame (df_word_telefonia_averias)
                matched_rows = df_merge_word_telefonia_corte_excel.filter(
                    col("nro_incidencia").isNotNull() & 
                    col("Fecha y Hora Inicio_corte_excel").isNull()
                )
            elif match_type == 'both':
                # Rows that exist in both DataFrames
                matched_rows = df_merge_word_telefonia_corte_excel.filter(
                    col("nro_incidencia").isNotNull() & 
                    col("Fecha y Hora Inicio_corte_excel").isNotNull()
                )
            elif match_type == 'right_only':
                # Rows that exist only in the right DataFrame (df_corte_excel)
                matched_rows = df_merge_word_telefonia_corte_excel.filter(
                    col("nro_incidencia").isNotNull() & 
                    col("Fecha y Hora Inicio_corte_excel").isNotNull()
                )
            else:
                raise ValueError(f"Invalid match_type: {match_type}")

            return matched_rows

        except Exception as e:
            raise Exception(f"Error merging telefono averias data: {str(e)}")
        finally:
            # Cleanup
            if 'df_merge_word_telefonia_corte_excel' in locals():
                df_merge_word_telefonia_corte_excel.unpersist()
            spark.stop()

  