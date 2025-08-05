import pandas as pd
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions,
)

@log_exceptions
def o0_reclamo_no_en_cuadro_averias_validator(df: pd.DataFrame, componente_word: str) -> pd.DataFrame:
    """
    Checks for incidents of type 'RECLAMO' in the corte_excel file
    that are not present in the word_datos_averias file.
    """
    if df.empty:
        return pd.DataFrame()

    if componente_word == "COMPONENTE II":
        component_type = "datos"
    elif componente_word == "COMPONENTE IV":
        component_type = "telefonia"

    df['mensaje'] = f"La incidencia de tipo RECLAMO no se encuentra en el cuadro de averías del Word de {component_type}."
    df['objetivo'] = '2.0.1'
    
  
    result_df = df[['nro_incidencia', 'mensaje', 'objetivo', 'TIPO REPORTE']].copy()
    
    return result_df 