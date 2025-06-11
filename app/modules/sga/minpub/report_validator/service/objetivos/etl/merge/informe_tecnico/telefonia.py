from typing import List, Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, broadcast
from datetime import datetime, timedelta

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def merge_word_telefonia_informe_corte_excel(
        df_word_informe_tecnico_telefonia: DataFrame,
        df_corte_excel: DataFrame,
        match_type: str
    ) -> DataFrame:
    """
    Common merge function for Objective 2.

    Merges:
      - corte-excel with word_telefonia on 'nro_incidencia'

    Returns a merged DataFrame with common columns needed.
    """
    
    # Optimize join by broadcasting the smaller DataFrame
    df_merge_word_telefonia_corte_excel = df_word_informe_tecnico_telefonia.join(
        broadcast(df_corte_excel),  # Broadcast the smaller DataFrame to minimize shuffling
        on='nro_incidencia',
        how='left'
    ).withColumn(
        '_merge',
        when(col('nro_incidencia').isNotNull(), 'both').otherwise('left_only')
    )
       
    # Filter based on match_type
    matched_rows = df_merge_word_telefonia_corte_excel.filter(col('_merge') == match_type)
    
    return matched_rows

