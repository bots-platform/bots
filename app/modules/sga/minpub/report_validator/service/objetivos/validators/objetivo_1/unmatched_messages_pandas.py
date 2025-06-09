import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime


from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def build_message_merge_sga_335_corte_excel_unmatch( df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds a message for unmatched records between SGA-335 and Excel report.
    
    Args:
        df: DataFrame containing unmatched records with columns:
            - nro_incidencia
            - FECHA_Y_HORA_INICIO_fmt
            - FECHA_Y_HORA_FIN_fmt
            - TIPO REPORTE
            
    Returns:
        DataFrame with columns:
            - nro_incidencia
            - mensaje (error message)
            - TIPO REPORTE
            - objetivo (set to "1.0")
    """

    mensaje = ( "Nro Incidencia " + df['nro_incidencia'] + " con FECHA Y HORA INICIO DE CORTE EXCEL " +
    df['FECHA_Y_HORA_INICIO_fmt'] + " y FECHA HORA FIN DE CORTE EXCEL  " + 
    df['FECHA_Y_HORA_FIN_fmt']+  " no se encuentra en el reporte dinamico SGA - 335 para el rango de fecha de este corte \n"
    )

    df['mensaje'] = mensaje
    df['objetivo'] = "1.0"

    return df[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']] 