

import pandas as pd
import numpy as np

from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)


@log_exceptions
def validate_anexos_indisponibilidad_word( merged_df: pd.DataFrame, componente_word: str) -> pd.DataFrame:
    """
    Validate anexos indisponibilidad 
    Retun a Dataframe  with new Boolean
    """

    df = merged_df.copy()


    df['indisponibilidad_header_match'] = (
        df['indisponibilidad_header'].astype(str).str.strip()== df['clock_stops_paragraph_header']
    )

    df['indisponibilidad_periodos_match'] = (
        df['indisponibilidad_periodos'].astype(str).str.strip()== df['clock_stops_paragraph_periodos']
    )


    df['indisponibilidad_total_match'] = (
        df['indisponibilidad_total'].astype(str).str.strip()== df['clock_stops_paragraph_footer']
    )

    df['Validation_OK'] = df['indisponibilidad_header_match'] &  df['indisponibilidad_periodos_match'] & df['indisponibilidad_total_match']
    
    df['fail_count'] = (
        (~df['indisponibilidad_header_match']).astype(int)+ 
        (~df['indisponibilidad_periodos_match']).astype(int)+ 
        (~df['indisponibilidad_total_match']).astype(int)

    )
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
        
        (
            np.where(~df['indisponibilidad_header_match'],
                    " No coincide texto inicio de word indisponibilidad en anexos : " + df['indisponibilidad_header'].astype(str) +
                     " es diferente a sga :  " + df['clock_stops_paragraph_header'].astype(str) + ". ", "") +

             np.where(~df['indisponibilidad_periodos_match'],
                    " No coincide paradas de reloj de word indisponibilidad en anexos : " + df['indisponibilidad_periodos'].astype(str) +
                     " es diferente a sga  :  " + df['clock_stops_paragraph_periodos'].astype(str) + ". ", "") +

              np.where(~df['indisponibilidad_total_match'],
            " No coincide total horas sin acceso a la sede de word indisponibilidad en anexos : " + df['indisponibilidad_total'].astype(str) +
             " es diferente a sga  :  " + df['clock_stops_paragraph_footer'].astype(str) + ". ", "") 

        )

    )

    df['mensaje']  = mensajes
    df['objetivo'] = 3.1

    return df[df['fail_count'] > 0][['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]