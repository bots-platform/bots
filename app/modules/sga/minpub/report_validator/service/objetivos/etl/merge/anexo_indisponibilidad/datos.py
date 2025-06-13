from typing import List, Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, when, lit, coalesce

from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import log_exceptions

@log_exceptions
def merge_word_datos_anexos_disponibilidad_df_merged_sga(
        df_matched_corte_sga335_Sharepoint_cuismp_sga380: DataFrame,
        df_word_anexos_disponibilidad_datos: DataFrame,
        #match_type:str
    ) -> DataFrame:
    """
    Common merge function for Objective 2 using PySpark.

    Merges:
      - corte-excel with word_telefonia anexos diponibilidad on 'nro_incidencia'

    Args:
        df_matched_corte_sga335_Sharepoint_cuismp_sga380: DataFrame containing matched SGA data
        df_word_anexos_disponibilidad_datos: DataFrame containing anexos disponibilidad data

    Returns:
        DataFrame: Merged DataFrame with common columns needed
    """
    with spark_manager.get_session_context() as spark:
        # Perform left join using Spark SQL
        df_merge_word_datos_anexos_disponibilidad_df_merged_sga = (
            df_matched_corte_sga335_Sharepoint_cuismp_sga380
            .join(
                df_word_anexos_disponibilidad_datos,
                on='nro_incidencia',
                how='left'
            )
        )

        df_merge_word_datos_anexos_disponibilidad_df_merged_sga.cache()

        return df_merge_word_datos_anexos_disponibilidad_df_merged_sga

    
