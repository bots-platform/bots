
import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime


from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)

@log_exceptions
def validation_averia_tipificacion_problema(merged_df:pd.DataFrame) -> pd.DataFrame:
    """
    Valdate that:
    - Averia (Excel -CORTE) either match tipificacion del problema directly
    - OR The first 37 characteres  match, if direct comparasion fails.

    Returns a dataframe with boolean flag and a 'fail_count' colum.
    """
    df = merged_df.copy()

    df['averia_direct_match'] = (df['AVERÍA'] == df['tipificacion_problema']) 
    df['averia_partial_37'] = (df['AVERÍA'].str[:37] == df['tipificacion_problema'].str[:37])

    df['Validation_OK'] = df['averia_direct_match'] | df['averia_partial_37']
    df['fail_count'] = (~df['Validation_OK']).astype(int)

    return df

@log_exceptions
def build_failure_messages_averia_tipificacion_tipo(df:pd.DataFrame) -> pd.DataFrame:
    """"
    Build descriptive failure menssages for rows that don't pass validation. 
    Returns Dataframe with only failing rows.
    """

    failed_rows = df[~df['Validation_OK']].copy()

    if not failed_rows.empty:
        failed_rows['mensaje'] = ("\n\n No coincide AVERÍA en EXCEL-CORTE:  ("+df['AVERÍA'].astype(str)+ 
        ") con tipificacion problema en SGA 335: (" + df["tipificacion_problema"].astype(str) +
        "). ") 
        failed_rows['objetivo'] = "1.6"

    return failed_rows[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]


