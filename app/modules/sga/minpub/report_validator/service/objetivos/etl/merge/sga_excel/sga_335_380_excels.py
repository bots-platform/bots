from typing import List, Dict
import pandas as pd
from datetime import datetime, timedelta

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

from app.modules.sga.minpub.report_validator.service.objetivos.utils.formattings import ( 
    make_paragraph_paradas_cliente, make_paragraph_paradas_cliente_total, make_paragraph_paradas_cliente_header,
    make_paragraph_paradas_cliente_periodos, make_paragraph_paradas_cliente_footer
)

from app.modules.sga.minpub.report_validator.service.objetivos.utils.calculations import ( 
    calculate_total_clock_stop_minutes_by_incidencia,
    calculate_total_clock_stop_by_incidencia
)

@log_exceptions
def merge_sga_335_corte_excel_sharepoint_cuismp_sga380(
        df_corte_excel: pd.DataFrame, 
        df_sga_dinamico_335: pd.DataFrame,
        df_cid_cuismp_sharepoint: pd.DataFrame,
        df_sga_dinamico_380: pd.DataFrame,
        match_type:str
    ) -> pd.DataFrame:
        """
        Common merge function for Objective 1.

        Merges:
          - corte-excel  with sga_dinamico_335 on 'nro_incidencia'

        Returns a merged DataFrame with common columns needed.
        """
        
        df_merged_sga335_excel = pd.merge(
            df_corte_excel,
            df_sga_dinamico_335,
            on='nro_incidencia',
            how='left',
            indicator=True,
            suffixes=('_corte_excel', '_sga_dinamico_335')
        )

        df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp = pd.merge(
        df_merged_sga335_excel,
        df_cid_cuismp_sharepoint,
        on='cid',
        how='left',
        suffixes=('_sga_dinamico_335_excel_matched', '_sharepoint_cid_cuismp')
        )

        df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp['sum_paradas'] = df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp.apply(
            lambda r: calculate_total_clock_stop_minutes_by_incidencia(
                nro_incidencia = r["nro_incidencia"],
                interruption_start = r["interrupcion_inicio"],
                interruption_end = r["interrupcion_fin"],
                df_sga_paradas = df_sga_dinamico_380
            ),
            axis= 1
        )

        df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp['clock_stops'] = df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp.apply(
            lambda r: calculate_total_clock_stop_by_incidencia(
                nro_incidencia = r["nro_incidencia"],
                interruption_start = r["interrupcion_inicio"],
                interruption_end = r["interrupcion_fin"],
                df_sga_paradas = df_sga_dinamico_380
            ),
            axis= 1
        )

        df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp['clock_stops_paragraph'] = df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp['clock_stops'].apply(make_paragraph_paradas_cliente)
        df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp['clock_stops_paragraph_header'] = df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp['clock_stops'].apply(make_paragraph_paradas_cliente_header)
        df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp['clock_stops_paragraph_periodos'] = df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp['clock_stops'].apply(make_paragraph_paradas_cliente_periodos)
        df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp['clock_stops_paragraph_footer'] = df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp['clock_stops'].apply(make_paragraph_paradas_cliente_total)


        matched_rows = df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp[df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp['_merge'] == match_type]

        return matched_rows
  