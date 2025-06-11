
import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime


from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
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

    df['averia_direct_match'] = (df['AVERÍA'].astype(str).str.strip() == df['tipificacion_problema'].astype(str).str.strip())
    df['averia_partial_37_match'] = (df['AVERÍA'].astype(str).str.strip().str[:37] == df['tipificacion_problema'].astype(str).str.strip().str[:37])

    df['averia_match'] = df['averia_direct_match'] | df['averia_partial_37_match']

    df['Validation_OK'] = df['averia_match']
    df['fail_count'] = (~df['Validation_OK']).astype(int)

    return df

@log_exceptions
def build_failure_messages_averia_tipificacion_problema(df:pd.DataFrame) -> pd.DataFrame:
    """"
    Build descriptive failure menssages for rows that don't pass validation. 
    Returns Dataframe with only failing rows.
    """

    mensaje = np.where(
        df['Validation_OK'],
        " Tipificacion problema  coincide",
        (
        np.where(~df['averia_match'],
            "\n No coincide columnna AVERÍA en EXCEL-CORTE: \n" +
            df['AVERÍA'].astype(str)+"\n con tipificacion_problema en SGA 335: \n"+ 
            df['tipificacion_problema'].astype(str), "")           
         )
        )


    df['mensaje'] = mensaje
    df['objetivo'] = "1.6"
    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]




