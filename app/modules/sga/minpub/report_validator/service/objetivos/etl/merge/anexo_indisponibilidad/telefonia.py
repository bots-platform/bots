from typing import List, Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, when, lit, coalesce

from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import log_exceptions

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
    with spark_manager.get_session_context() as spark:
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
            
        # cols_a_reemplazar = ['indisponibilidad_header', 'indisponibilidad_periodos', 'indisponibilidad_total']
        # df_merge_word_telefonia_df_merged_sga = df_merge_word_telefonia_df_merged_sga.na.fill('no encontrado', subset=cols_a_reemplazar)

        return df_merge_word_telefonia_df_merged_sga

    
