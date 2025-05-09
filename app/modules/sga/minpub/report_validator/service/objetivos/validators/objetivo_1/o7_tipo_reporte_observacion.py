import pandas as pd
import numpy as np
import re

from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def validation_tipo_reporte_observacion(merged_df:pd.DataFrame)-> pd.DataFrame:
    """
    Validation requeriments for tipo_reporte and observacion columns:

    When TIPO REPORTE IS "RECLAMO":
    - tipo_caso must be in {"SIN SERVICIO", "ENLACE LENTO", "SIN SERVICIO-NO DA TONO", "OTROS CALIDAD"}
    - tipo_caso must not end with "MONITOREO"
    - MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS" must no start with "A través de los Sistemas de Monitoreo de Claro, de manera proactiva se identificó... )
    - OBSERVACION must start with "Se generó ticket para la revisión del servicio de {internet/datos/telefonía} de la sede"

    When tipo_reporte is "PROACTIVO"
    -tipo_caso must be end in MONITOREO
    - MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS" must start with "A través de los Sistemas de Monitoreo de Claro, de manera proactiva se identificó... )
    - OBSERVACION must start with "Se generó código de atención (interno) para la revisión del servicio de {internet/datos/telefonía} de la sede"

    Return  a Dataframe with validation flags and fail counts
    """
    df = merged_df.copy()

    valid_tipo_caso_reclamo = {"SIN SERVICIO", "ENLACE LENTO", "SIN SERVICIO-NO DA TONO", "OTROS CALIDAD"}
    
    pattern = re.compile(
        r'(?i)^a través de los sistemas de monitoreo de (?-i:Claro), de manera proactiva se identificó'
    )





    df['observacion_pattern_componente_tipo_reporte'] = ""

    componente_patterns = {
        ('RECLAMO', 'COMPONENTE II') : "Se generó ticket para la revisión del servicio de datos de la sede {}",
        ('RECLAMO', 'COMPONENTE III') : "Se generó ticket para la revisión del servicio de datos de la sede {}",
        ('RECLAMO', 'COMPONENTE IV') : "Se generó ticket para la revisión del servicio de telefonía de la sede {}",
        ('PROACTIVO', 'COMPONENTE II') : "Se generó ticket para la revisión del servicio de datos de la sede {}",
        ('PROACTIVO', 'COMPONENTE III') : "Se generó ticket para la revisión del servicio de datos de la sede {}",
        ('PROACTIVO', 'COMPONENTE IV') : "Se generó ticket para la revisión del servicio de telefonía de la sede {}"
    }

    for (tipo_reporte, tipo_componente), pattern_tempalte in componente_patterns.items():
        mask = (df['TIPO REPORTE'] == tipo_reporte) & (df['COMPONENTE'] == tipo_componente)
        df.loc[mask, 'observacion_pattern_componente_tipo_reporte'] = [
            pattern_tempalte.format(sede) for sede in df.loc[mask, 'SEDE']
        ]

    validation_columns = [
        'reclamo_tipo_caso_valid', 'reclamo_no_monitoreo', 'reclamo_medidas_correctivas_valid',
        'reclamo_observacion_valid', 'proactivo_tipo_caso_valid', 'proactivo_medidas_correctivas_valid',
        'proactivo_observacion_valid'
    ]
    for col in validation_columns:
        df[col] = True



    reclamo_mask = df['TIPO REPORTE'] == 'RECLAMO'
    if reclamo_mask.any():
        medidas = df.loc[reclamo_mask, 'MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS']
        # .str.match aplica el regex al comienzo de la cadena
        match_mask = medidas.str.match(pattern)
        # Valid es True cuando NO arranca con ese texto
        df.loc[reclamo_mask, 'reclamo_medidas_correctivas_valid'] = ~match_mask



    reclamo_mask = df['TIPO REPORTE'] == 'RECLAMO'
    proactivo_mask = df['TIPO REPORTE'] == 'PROACTIVO'
    
    if reclamo_mask.any():
        df.loc[reclamo_mask, 'reclamo_tipo_caso_valid'] = df.loc[reclamo_mask, 'tipo_caso'].isin(valid_tipo_caso_reclamo)
        df.loc[reclamo_mask, 'reclamo_no_monitoresoruo'] = ~df.loc[reclamo_mask, 'tipo_caso'].str.endswith('MONITOREO')
        
        medidas = df.loc[reclamo_mask, 'MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS']
        match_mask = medidas.str.match(pattern)
        df.loc[reclamo_mask, 'reclamo_medidas_correctivas_valid'] = ~match_mask
        
        
        df.loc[reclamo_mask, 'reclamo_observacion_valid'] = df.loc[reclamo_mask, 'OBSERVACIÓN'].str.lower() == df.loc[reclamo_mask, 'observacion_pattern_componente_tipo_reporte'].str.lower()

    if proactivo_mask.any():
        df.loc[proactivo_mask, 'proactivo_tipo_caso_valid'] = df.loc[proactivo_mask, 'tipo_caso'].str.endswith('MONITOREO')

      

        medidas = df.loc[proactivo_mask, 'MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS']
        match_mask = medidas.str.match(pattern)
        df.loc[proactivo_mask, 'proactivo_medidas_correctivas_valid'] = match_mask
        
        df.loc[proactivo_mask, 'proactivo_observacion_valid'] = df.loc[proactivo_mask, 'OBSERVACIÓN'].str.lower() == df.loc[proactivo_mask, 'observacion_pattern_componente_tipo_reporte'].str.lower()



    df['tipo_reporte_all_valid'] = False

    if reclamo_mask.any():
        df.loc[reclamo_mask, 'tipo_reporte_all_valid'] = (
            df.loc[reclamo_mask, 'reclamo_tipo_caso_valid'] &
            df.loc[reclamo_mask, 'reclamo_no_monitoreo'] &
            df.loc[reclamo_mask,'reclamo_medidas_correctivas_valid'] &
            df.loc[reclamo_mask, 'reclamo_observacion_valid']
        )

    if proactivo_mask.any():
         df.loc[proactivo_mask, 'tipo_reporte_all_valid'] = (
            df.loc[proactivo_mask, 'proactivo_tipo_caso_valid'] &
            df.loc[proactivo_mask, 'proactivo_medidas_correctivas_valid'] &
            df.loc[proactivo_mask,'proactivo_observacion_valid'] 
        )
         
    neither_mask = ~(reclamo_mask | proactivo_mask)
    if neither_mask.any():
        df.loc[neither_mask, 'tipo_reporte_all_valid'] = True

    df['Validation_OK'] = df['tipo_reporte_all_valid']
    df['fail_count'] = (~df['Validation_OK']).astype(int)


    return df

@log_exceptions
def build_failure_messages_reporte_observacion(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds specific failure messages for TIPO REPORTE and OBSERVACION validation  issues
    using np.where for improved performance.

    Returns Dataframe with columns ['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo'] for failing records.
    """ 



    if not isinstance(df, pd.DataFrame) or df.empty or 'Validation_OK' not in df.columns:
        return pd.DataFrame(columns=['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo'])
    
    df['formatted_pattern'] = df.apply(
        lambda row: row['observacion_pattern_componente_tipo_reporte'].format(SEDE=row['SEDE'])
        if isinstance(row['observacion_pattern_componente_tipo_reporte'], str) else "",
        axis=1
    ) 
    
    mensaje = np.where(
        df['Validation_OK'],
        "Validation de TIPO REPORTE y OBSERVACION exitosa",
        (
            np.where((df['TIPO REPORTE'] == 'RECLAMO') & (~df['reclamo_tipo_caso_valid']),
                     "\n\n EN EXCEL-CORTE  RECLAMO columna TIPO CASO:  \n\n" + df['tipo_caso'].astype(str) + 
                     " \n\n no está en lista permitida : (SIN SERVICIO , ENLACE LENTO, SIN SERVICIO-NO DA TONO , OTROS CALIDAD).",
                    "") +

            np.where((df['TIPO REPORTE'] == 'RECLAMO') & (~df['reclamo_no_monitoreo']),
                     "\n\n EN EXCEL-CORTE RECLAMO con columna TIPO CASO: \n\n " + df['tipo_caso'].astype(str) + "\n\n no debe terminar en 'MONITOREO'.",
                    "") +

            np.where((df['TIPO REPORTE'] == 'RECLAMO') & (~df['reclamo_medidas_correctivas_valid']),
                     "\n No coincide  EXCEL-CORTE RECLAMO columna MEDIDAS CORRECTIVAS : \n"+ df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'] +
                     "\n  no debe iniciar con 'A través de los Sistemas de Monitoreo de Claro, de manera proactiva se identificó'.",
                    "") +

            np.where((df['TIPO REPORTE'] == 'RECLAMO') & (~df['reclamo_observacion_valid']),
                     "\n EN EXCEL-CORTE RECLAMO columna OBSERVACION: debe iniciar con \n" + df['observacion_pattern_componente_tipo_reporte'].astype(str) + 
                     "\n pero se obtuvo: \n " + df['OBSERVACIÓN'].astype(str) ,
                    "") +

            np.where((df['TIPO REPORTE'] == 'PROACTIVO') & (~df['proactivo_tipo_caso_valid']),
                     "\n\n EN EXCEL-CORTE PROACTIVO columna TIPO CASO : \n\n" + df['tipo_caso'].astype(str) + "\n\n debe terminar en 'MONITOREO'.",
                    "") +

            np.where((df['TIPO REPORTE'] == 'PROACTIVO') & (~df['proactivo_medidas_correctivas_valid']),
                     "\n EN EXCEL-CORTE PROACTIVO columna MEDIDAS CORRECTIVAS:  \n" + df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS']+ 
                     " \n debe iniciar con 'A través de los Sistemas de Monitoreo de Claro, de manera proactiva se identificó'.",
                    "") +

            np.where((df['TIPO REPORTE'] == 'PROACTIVO') & (~df['proactivo_observacion_valid']),
                    "\n EN EXCEL-CORTE PROACTIVO columna OBSERVACION  debe ser: \n"  + df['observacion_pattern_componente_tipo_reporte'].astype(str) +
                      "\n  pero se obtuvo \n"  +  df['OBSERVACIÓN'].astype(str),
                    "") 
        )
    )

    df['mensaje'] = mensaje
    df['objetivo'] = "1.7"

    df_failures = df[df['fail_count'] > 0 ]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]
  
