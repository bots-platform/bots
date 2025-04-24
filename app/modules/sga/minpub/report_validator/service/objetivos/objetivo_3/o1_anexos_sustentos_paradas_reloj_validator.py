

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

