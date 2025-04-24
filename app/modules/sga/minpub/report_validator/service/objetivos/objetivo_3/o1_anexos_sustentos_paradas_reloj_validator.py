

import pandas as pd
import numpy as np
from utils.logger_config import get_sga_logger
from typing import Tuple


logger = get_sga_logger()

def log_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper



@log_exceptions
def validate_anexos_indisponibilidad_word( merged_df: pd.DataFrame, componente_word: str) -> pd.DataFrame:
    """
    Validate anexos indisponibilidad 
    Retun a Dataframe  with new Boolean
    """

    df = merged_df.copy()

    df['expected_indisponibilidad'] = df['clock_stops_paragraph']
    df['indisponibilidad_ok'] = (
        df['indisponibilidad_extract'].astype(str).str.strip()
        == df['expected_indisponibilidad']
    )

    df['Validation_OK'] = df['indisponibilidad_ok']
    df['fail_count']   = (~df['Validation_OK']).astype(int)
    return df



@log_exceptions
def build_failure_messages_validate_anexos_indisponibilidad_word(df: pd.DataFrame) -> pd.DataFrame:
    
    """
    Returns a DataFrame of failures with columns:
    ['nro_incidencia','mensaje','objetivo']
    """
    if df is None or df.empty or 'Validation_OK' not in df.columns:
        return pd.DataFrame(columns=['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo'])

    mensajes = np.where(
        df['Validation_OK'],
        "Validación exitosa: ANEXOS INDISPONIBILIDAD coincide con las paradas de reloj",
        
        " ANEXO INDISPONIBILIDAD  inválida:\n"
        + " No coincide indisponibilidad en anexos:\n"
        + df['indisponibilidad_extract'].astype(str)
        + "\n\n✔  Indisponibilidad esperada :\n"
        + df['expected_indisponibilidad']
    )

    df['mensaje']  = mensajes
    df['objetivo'] = 3.1

    return df[df['fail_count'] > 0][['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]