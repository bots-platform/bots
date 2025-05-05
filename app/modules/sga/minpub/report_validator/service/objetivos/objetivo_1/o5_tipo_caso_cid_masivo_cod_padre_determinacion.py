
import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)

import re
pattern = re.compile(r'(?:COMPONENTE|Componente|COMONENTE)\s*(?:IV|II|III|I|V)(?:\s*-|\s*)\s*', re.IGNORECASE)

@log_exceptions
def remove_componente_prefix(text: str) -> str:
    """
    Remove a prefix like  'COMPONENETE - II' (or variations) from the start of the string
    Also removes extra spaces o dashes.
    """ 
    if not isinstance(text, str):
        return ""
    text = text.replace('\r', ' ')
    text = text.replace('_x000D_', '').strip()
    cleaned = pattern.sub("", text).strip()
    return cleaned

def remove_special_caracteres(text: str) -> str:
    if not isinstance(text, str):
        return ""
    cleaned = text.replace('_x000D_', '').strip()
    return cleaned

@log_exceptions
def validation_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa(merged_df: pd.DataFrame) -> pd.DataFrame:
    df = merged_df.copy()

    df['causa_excel_clean'] = df['DETERMINACIÓN DE LA CAUSA'].apply(remove_special_caracteres)
    df['causa_sga335_clean'] = df['it_determinacion_de_la_causa'].apply(remove_componente_prefix)

    df['causa_match'] =  df['causa_excel_clean'] == df['causa_sga335_clean']
    df['tipo_caso_match'] = df['tipo_caso'] == df['TIPO CASO']
    df['cid_match'] = df['cid'] == df['CID']
    df['cod_incidencia_padre_match'] = df['codincidencepadre'] == df['CODINCIDENCEPADRE'] 


    df['Validation_OK'] = (
        df['causa_match'] &
        df['tipo_caso_match'] &
        df['cid_match'] &
        df['cod_incidencia_padre_match']
    )
    
    df['fail_count'] =(
       (~df['causa_match']).astype(int) + 
       (~df['tipo_caso_match']).astype(int) + 
       (~df['cid_match']).astype(int) + 
       (~df['cod_incidencia_padre_match']).astype(int) 
    ) 

    return df

@log_exceptions
def build_failure_messages_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa(df:pd.DataFrame) -> pd.DataFrame:
    mensaje = np.where(
        df['Validation_OK'],
        "Determinacion de la causa coincide  despues de remover 'COMPONENTE ' prefix",
         (
            np.where(~df['causa_match'],
                     " \n\n No coincide Determinacion de la causa Excel-Corte: \n\n" +
                       df['causa_excel_clean'].astype(str)+"\n\n con SGA Determinación de la causa: \n\n"+ df['causa_sga335_clean'],  "")+
            np.where(~df['tipo_caso_match'],
                     "\n\n No coincide Tipo de Caso de CORTE-EXCEL: \n\n" + df['TIPO CASO'].astype(str) +"\n\n con SGA determinación de la causa: \n\n" + 
                     df['tipo_caso'].astype(str)+ "). ", "")+
            np.where(~df['cid_match'],
                     "\n\n No coincide cid  de CORTE-EXCEL : \n\n"+ df['CID'].astype(str) +
                     "\n\n con SGA: \n\n" + df['cid'].astype(str) +
                      "). ", "")+
           np.where(~df['cod_incidencia_padre_match'],
                     "\n\n  No coincide nro incidencia padre EXCEL-CORTE: \n\n" + df['CODINCIDENCEPADRE'].astype(str) +
                       "con columna SGA codeincidencepadre: \n\n" +df['codincidencepadre'].astype(str)+ "). ", "")
        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = "1.5"
    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]

