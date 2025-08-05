from typing import List, Dict
import pandas as pd
from datetime import datetime, timedelta

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def merge_word_datos_anexos_disponibilidad_df_merged_sga(
        df_matched_corte_sga335_Sharepoint_cuismp_sga380: pd.DataFrame,
        df_word_anexos_disponibilidad_datos: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Common merge function for Objective 2.

        Merges:
          - corte-excel  with word_telefonia anexos diponibilidad on 'nro_incidencia'

        Returns a merged DataFrame with common columns needed.
        """
        df_merge_word_datos_anexos_disponibilidad_df_merged_sga = pd.merge(
        df_matched_corte_sga335_Sharepoint_cuismp_sga380,
        df_word_anexos_disponibilidad_datos,
        on='nro_incidencia',
        how='left',
        suffixes=('_word_datos_anexos_indisp' , '_dfs_merged')
        )
       
        cols_a_reemplazar = ['indisponibilidad_header', 'indisponibilidad_periodos', 'indisponibilidad_total']

        df_merge_word_datos_anexos_disponibilidad_df_merged_sga[cols_a_reemplazar] = df_merge_word_datos_anexos_disponibilidad_df_merged_sga[cols_a_reemplazar].fillna('no encontrado')

        return df_merge_word_datos_anexos_disponibilidad_df_merged_sga

    

