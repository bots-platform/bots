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
            .appName("AnexoIndisponibilidadTelefoniaProcessor")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate())

@log_exceptions
def merge_word_telefonia_anexos_disponibilidad_df_merged_sga(
        df_matched_corte_sga335_Sharepoint_cuismp_sga380: DataFrame,
        df_word_anexo_disponibilidad_telefonia: DataFrame,
        #match_type:str
    ) -> DataFrame:
        """
        Common merge function for Objective 2 using PySpark.

        Merges:
          - corte-excel with word_telefonia on 'nro_incidencia'

        Args:
            df_matched_corte_sga335_Sharepoint_cuismp_sga380: DataFrame containing matched SGA data
            df_word_anexo_disponibilidad_telefonia: DataFrame containing anexos disponibilidad telefono data

        Returns:
            DataFrame: Merged DataFrame with common columns needed
        """
        spark = get_spark_session()
        try:
            # Perform left join using Spark SQL
            df_merge_word_telefonia_df_merged_sga = (
                df_matched_corte_sga335_Sharepoint_cuismp_sga380
                .join(
                    df_word_anexo_disponibilidad_telefonia,
                    on='nro_incidencia',
                    how='left'
                )
            )

            # Cache the result for better performance if it will be used multiple times
            df_merge_word_telefonia_df_merged_sga.cache()

            # Note: The commented code below was in the original pandas version
            # If needed, we can implement similar functionality using Spark:
            # cols_a_reemplazar = ['indisponibilidad_header', 'indisponibilidad_periodos', 'indisponibilidad_total']
            # df_merge_word_telefonia_df_merged_sga = df_merge_word_telefonia_df_merged_sga.na.fill('no encontrado', subset=cols_a_reemplazar)

            return df_merge_word_telefonia_df_merged_sga

        except Exception as e:
            raise Exception(f"Error merging anexos disponibilidad telefono data: {str(e)}")
        finally:
            # Cleanup
            if 'df_merge_word_telefonia_df_merged_sga' in locals():
                df_merge_word_telefonia_df_merged_sga.unpersist()
            spark.stop()

    
