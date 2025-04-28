from typing import List, Dict
import pandas as pd
from datetime import datetime, timedelta

from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)


@log_exceptions
def merge_word_telefonia_informe_corte_excel(
        df_word_informe_tecnico_telefonia: pd.DataFrame,
        df_corte_excel: pd.DataFrame,
        match_type:str
    ) -> pd.DataFrame:
        """
        Common merge function for Objective 2.

        Merges:
          - corte-excel  with word_telefonia on 'nro_incidencia'

        Returns a merged DataFrame with common columns needed.
        """
        
        df_merge_word_telefonia_corte_excel = pd.merge(
        df_word_informe_tecnico_telefonia,
        df_corte_excel,
        on='nro_incidencia',
        how='left',
        indicator=True,
        suffixes=('_word_telefonia_informe', '_corte_excel')
        )
           
        matched_rows = df_merge_word_telefonia_corte_excel[df_merge_word_telefonia_corte_excel['_merge'] == match_type]
        return matched_rows

