
import pandas as pd
import numpy as np

from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)

@log_exceptions
def validate_informe_tecnico_word( merged_df: pd.DataFrame, componente_word: str) -> pd.DataFrame:
    """
    Validate values columns coming from word and excel files
    Retun a Dataframe  with new Boolean
    
    Columnas en EXCEL	                                        Columnas en WORD DATOS

    "FECHA Y HORA INICIO",                                      "Fecha y hora inicio":   
    "FECHA Y HORA FIN",                                         "Fecha y hora fin": 
    "CUISMP",                                                   "CUISMP": 
    "TIPO CASO",                                                "Tipo Caso":                    
    "OBSERVACION",                                              "Observación": 
    "DETERMINACIÓN DE LA CAUSA",                                "DETERMINACIÓN DE LA CAUSA":  
    "MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS",              "MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS": 
    """

    df = merged_df.copy()

    if componente_word == 'COMPONENTE II':
        df['CUISMP_word'] = df['CUISMP_word_datos_informe']
    elif componente_word == 'COMPONENTE IV':
        df['CUISMP_word'] = df['CUISMP_word_telefonia_informe']

    if componente_word == 'COMPONENTE II':
        df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word'] = df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word_datos_informe']
    elif componente_word == 'COMPONENTE IV':
        df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word'] = df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word_telefonia_informe']


    if componente_word == 'COMPONENTE II':
        df['DETERMINACIÓN DE LA CAUSA_word'] = df['DETERMINACIÓN DE LA CAUSA_word_datos_informe']
    elif componente_word == 'COMPONENTE IV':
        df['DETERMINACIÓN DE LA CAUSA_word'] = df['DETERMINACIÓN DE LA CAUSA_word_telefonia_informe']



    df['Fecha_hora_inicio_match'] = df['FECHA_Y_HORA_INICIO_fmt'] == df['Fecha y hora inicio']
    df['fecha_hora_fin_match'] = df['FECHA_Y_HORA_FIN_fmt'] == df['Fecha y hora fin']
    df['CUISMP_match'] = df['CUISMP_corte_excel'] == df['CUISMP_word']
    df['tipo_caso_match'] = df['TIPO CASO'] == df['Tipo Caso']
    df['observacion_match'] = df['OBSERVACIÓN'] == df['Observación']
    df['dt_causa_match'] = df['DETERMINACIÓN DE LA CAUSA_corte_excel'] == df['DETERMINACIÓN DE LA CAUSA_word'] 
    df['medidas_correctivas_match'] = df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_corte_excel'] == df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word']

    df['Validation_OK'] = (
        df['Fecha_hora_inicio_match'] &
        df['fecha_hora_fin_match'] &
        df['CUISMP_match'] &
        df['tipo_caso_match'] &
        df['observacion_match'] &
        df['dt_causa_match'] &
        df['medidas_correctivas_match']
    )

    df['fail_count'] = (
        (~df['Fecha_hora_inicio_match']).astype(int)+ 
        (~df['fecha_hora_fin_match']).astype(int)+ 
        (~df['CUISMP_match']).astype(int)+ 
        (~df['tipo_caso_match']).astype(int)+ 
        (~df['observacion_match']).astype(int)+ 
        (~df['dt_causa_match']).astype(int)+ 
        (~df['medidas_correctivas_match']).astype(int)
    )
    return df


@log_exceptions
def build_failure_messages_validate_informe_tecnico_word(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds the 'mensaje' column using vectorized operations.
    Adds the 'objetivo2' column (constant value of 2) and filters
    rows that fail at least one validation.
    
    Returns a DataFrame with:
      ['nro_incidencia', 'mensaje', 'objetivo']

    """
    mensaje = np.where(
        df['Validation_OK'],
        "Validation successful",
        (

            np.where(~df['Fecha_hora_inicio_match'],
                     " No coincide Fecha y hora inicio de WORD informe técnico : " + df['Fecha y hora inicio'].astype(str) +
                     " es diferente a EXCEL-CORTE:  " + df['FECHA_Y_HORA_INICIO_fmt'].astype(str) + ". ", "") +

            np.where(~df['fecha_hora_fin_match'],
                     " No coincide Fecha y hora fin de WORD informe técnico : " + df['Fecha y hora fin'].astype(str) +
                     " es diferente a EXCEL-CORTE:  " + df['FECHA_Y_HORA_FIN_fmt'].astype(str) + ". ", "") +

            np.where(~df['CUISMP_match'],
                     " No coincide CUISMP_word_telefonia de WORD informe técnico : " + df['CUISMP_word'].astype(str) +
                     " es diferente a CUISMP_corte_excel: " + df['CUISMP_corte_excel'].astype(str) + ". ", "") +

            np.where(~df['tipo_caso_match'],
                     " No coincide Avería reportada de WORD informe técnico : " + df['Tipo Caso'].astype(str) +
                     " es diferente a TIPO CASO de Excel: " + df['TIPO CASO'].astype(str) + ". ", "") +
                    
            
            np.where(~df['observacion_match'],
                     " No coincide Observacion de WORD informe técnico : " + df['Observación'].astype(str) +
                     " es diferente a OBSERVACIÓN de Excel: " + df['OBSERVACIÓN'].astype(str) + ". ", "") +

        
            np.where(~df['dt_causa_match'],
                     " No coincide Determinación de la causa de WORD informe técnico : " + df['DETERMINACIÓN DE LA CAUSA_word'].astype(str) +
                     " es diferente a DETERMINACION DE LA CAUSA de Excel: " + df['DETERMINACIÓN DE LA CAUSA_corte_excel'].astype(str) + ". ", "") +


            np.where(~df['medidas_correctivas_match'],
                     " No coincide MEDIDAS CORRECTIVAS de WORD informe técnico : " + df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word'].astype(str) +
                     "\n\n es diferente a MEDIDAS CORRECTIVAS de Excel: " + df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_corte_excel'].astype(str) + ". ", "") 

        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = "2.2"
    
    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]



