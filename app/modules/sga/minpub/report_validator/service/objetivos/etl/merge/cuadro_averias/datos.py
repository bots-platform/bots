from typing import List, Dict
from pyspark.sql import DataFrame
from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def merge_word_datos_averias_corte_excel(
        df_word_datos_averias: DataFrame,
        df_corte_excel: DataFrame,
        match_type: str
    ) -> DataFrame:
    """
    Common merge function for Objective 2 using PySpark.

    Merges:
      - corte-excel with word_telefonia on 'nro_incidencia'

    Returns a merged DataFrame with common columns needed.
    """
    with spark_manager.get_session():
        df_merge_word_datos_corte_excel = df_word_datos_averias.join(
            df_corte_excel,
            on='nro_incidencia',
            how='left'
        )
 
        return df_merge_word_datos_corte_excel

    