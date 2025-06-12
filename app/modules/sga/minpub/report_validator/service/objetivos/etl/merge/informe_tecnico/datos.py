from typing import List, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
from datetime import datetime, timedelta

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

from app.modules.sga.minpub.report_validator.service.objetivos.utils.spark_manager import spark_manager

@log_exceptions
def merge_word_datos_informe_corte_excel(
        df_word_informe_tecnico_datos: DataFrame,
        df_corte_excel: DataFrame,
        match_type: str
    ) -> DataFrame:
    """
    Common merge function for Objective 2 using PySpark.

    Merges:
      - corte-excel with word_telefonia on 'nro_incidencia'

    Returns a merged DataFrame with common columns needed.
    """
    with spark_manager.get_session_context() as spark:
        try:
            # Perform the join operation with proper merge indicator
            df_merge_word_datos_corte_excel = df_word_informe_tecnico_datos.join(
                df_corte_excel,
                on='nro_incidencia',
                how='left'
            ).withColumn(
                '_merge',
                when(col('nro_incidencia').isNotNull(), 'both').otherwise('left_only')
            )

            # Filter based on match_type
            matched_rows = df_merge_word_datos_corte_excel.filter(col('_merge') == match_type)

            return matched_rows

        except Exception as e:
            raise Exception(f"Error merging datos informe: {str(e)}")
        finally:
            # Cleanup any cached DataFrames if needed
            if 'df_merge_word_datos_corte_excel' in locals():
                df_merge_word_datos_corte_excel.unpersist()

