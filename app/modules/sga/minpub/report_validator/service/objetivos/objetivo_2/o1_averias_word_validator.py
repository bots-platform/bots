# objetivo_2_validator.py

import pandas as pd
import numpy as np

from typing import Tuple


from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)

@log_exceptions
def validate_averias_word( merged_df: pd.DataFrame, componente_word: str) -> pd.DataFrame:
    """
    Validate values columns coming from word and excel files
    Retun a Dataframe  with new Boolean
    
    Columnas en EXCEL	        Columnas en WORD DATOS

    TICKET	                    Número de ticket
    FECHA Y HORA INICIO	        Fecha y Hora Inicio
    FECHA Y HORA FIN	        Fecha y Hora Fin
    CUISMP	                    CUISMP
    TIPO CASO	                Avería reportada
    AVERÍA	                    Causa
    TIEMPO (HH:MM)	            Tiempo real de afectación (HH:MM)
    COMPONENTE	                Componente
    DF	                        Distrito Fiscal
    FIN-INICIO (HH:MM)	        Tiempo Total (HH:MM)
    DETERMINACION DE LA CAUSA	Determinación de la causa
    RESPONSABILIDAD	            Responsable

    """
    df = merged_df.copy()

    df['Componente'] = componente_word

    column_name_cuismp = ""
    if componente_word == 'COMPONENTE II':
        column_name_cuismp = 'CUISMP_word_datos_averias'
    elif componente_word == 'COMPONENTE IV':
        column_name_cuismp = 'CUISMP_word_telefonia_averias'
    
    df['cuismp_word_averia'] = df[column_name_cuismp]


    df['Fecha_hora_inicio_match'] = df['FECHA Y HORA INICIO'] == df['Fecha y Hora Inicio']
    df['fecha_hora_fin_match'] = df['FECHA Y HORA FIN'] == df['Fecha y Hora Fin']    



    df['CUISMP_match'] = df['CUISMP_corte_excel'] == df['cuismp_word_averia']
    df['tipo_caso_match'] = df['TIPO CASO'] == df['Avería reportada']
    df['averia_match'] = df['AVERÍA'] == df['Causa']
    df['tiempo_hhmm_match'] = df['TIEMPO (HH:MM)_trimed'] == df['Tiempo real de afectación (HH:MM)']
    df['componente_match'] = df['COMPONENTE'] == df['Componente']
    df['df_match'] = df['DF'] == df['Distrito Fiscal']
    print("columnas disponibles: ", df.columns.tolist())
    df['fin_inicio_hhmm_match'] = df['FIN-INICIO (HH:MM)_trimed'] == df['Tiempo Total (HH:MM)']
    #df['dt_causa_match'] = df['DETERMINACION DE LA CAUSA']	== df['Determinación de la causa']
    df['responsabilidad_match'] = df['RESPONSABILIDAD'] == df['responsable']



    df['Validation_OK'] = (
        # df['TICKET_match'] &
        df['Fecha_hora_inicio_match'] &
        df['fecha_hora_fin_match'] &
        df['CUISMP_match'] &
        df['tipo_caso_match'] &
        df['averia_match'] &
        df['tiempo_hhmm_match'] &
        df['componente_match'] &
        df['df_match'] &
        df['fin_inicio_hhmm_match'] &
        # df['dt_causa_match'] &
        df['responsabilidad_match']
    )

    df['fail_count'] = (~df['Validation_OK']).astype(int)
    return df
 

@log_exceptions
def build_failure_messages_validate_averias_word(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds the 'mensaje' column using vectorized operations.
    Adds the 'objetivo2' column (constant value of 2) and filters
    rows that fail at least one validation.
    
    Returns a DataFrame with:
      ['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']

    """
    mensaje = np.where(
        df['Validation_OK'],
        "Validation successful",
        (
            np.where(~df['Fecha_hora_inicio_match'],
                     "\n\n  No coincide Fecha y Hora Inicio de WORD CUADRO AVERIAS : \n\n" + df['Fecha y Hora Inicio'].astype(str) +
                     "\n\n es diferente a FECHA Y HORA INICIO en EXCEL-CORTE:  \n\n" + df['FECHA_Y_HORA_INICIO_fmt'].astype(str) + ". ", "") +

            np.where(~df['fecha_hora_fin_match'],
                     "\n\n  No coincide Fecha y Hora Fin de WORD CUADRO AVERIAS : \n\n" + df['Fecha y Hora Fin'].astype(str) +
                     "\n\n es diferente columna FECHA Y HORA FIN en EXCEL-CORTE:  \n\n" + df['FECHA_Y_HORA_FIN_fmt'].astype(str) + ". ", "") +

            np.where(~df['CUISMP_match'],
                     "\n\n  No coincide CUISMP en WORD CUADRO AVERIAS : \n\n" + df['cuismp_word_averia'].astype(str) +
                     "\n\n es diferente a CUISMP en CORTE-EXCEL: \n\n" + df['CUISMP_corte_excel'].astype(str) + ". ", "") +

            np.where(~df['tipo_caso_match'],
                     "\n\n  No coincide Avería reportada de WORD CUADRO AVERIAS : \n\n" + df['Avería reportada'].astype(str) +
                     "\n\n es diferente a TIPO CASO en EXCEL-CORTE: \n\n" + df['TIPO CASO'].astype(str) + ". ", "") +
                    
            
            np.where(~df['averia_match'],
                     "\n\n  No coincide DETERMINACION DE LA CAUSE de WORD CUADRO AVERIAS : \n\n" + df['Causa'].astype(str) +
                     "\n\n es diferente a AVERÍA de CORTE-EXCEL: \n\n" + df['AVERÍA'].astype(str) + ". ", "") +

            np.where(~df['tiempo_hhmm_match'],
                     "\n\n  No coincide Tiempo real de afectación de WORD CUADRO DE AVERIAS: \n\n" + df['Tiempo real de afectación (HH:MM)'].astype(str) +
                     " es diferente a TIEMPO (HH:MM) de EXCEL-CORTE: " + df['TIEMPO (HH:MM)_trimed'].astype(str) + ". ", "") +


            np.where(~df['componente_match'],
                     " \n\n No coincide COMPONENTE en  de WORD CUADRO DE AVERIAS : \n\n" + df['Componente'].astype(str) +
                     "\n\n es diferente a la  columna COMPONENTE de CORTE-EXCEL: \n\n" + df['COMPONENTE'].astype(str) + ". ", "") +


            np.where(~df['df_match'],
                     "\n\n  No coincide Distrito Fiscal de WORD CUADRO DE AVERIAS : \n\n" + df['Distrito Fiscal'].astype(str) +
                     "\n\n es diferente a column DF de CORTE-EXCEL: \n\n" + df['DF'].astype(str) + ". ", "") +

             np.where(~df['fin_inicio_hhmm_match'],
                     " \n\n No coincide Tiempo Total (HH:MM) de WORD CUADRO DE AVERIAS : \n\n" + df['Tiempo Total (HH:MM)'].astype(str) +
                     "\n\n es diferente a la columna FIN-INICIO (HH:MM) de CORTE-EXCEL: \n\n" + df['FIN-INICIO (HH:MM)_trimed'].astype(str) + ". ", "") +


            # np.where(~df['dt_causa_match'],
            #          " No coincide Determinación de la causa de WORD-Datos : " + df['Determinación de la causa'].astype(str) +
            #          " es diferente a DETERMINACION DE LA CAUSA de Excel: " + df['DETERMINACION DE LA CAUSA'].astype(str) + ". ", "") +


            np.where(~df['responsabilidad_match'],
                     "\n\n  No coincide Responsable de WORD CUADRO DE AVERIAS : \n\n" + df['responsable'].astype(str) +
                     "\n\n es diferente a la columna RESPONSABILIDAD de CORTE-EXCEL: \n\n" + df['RESPONSABILIDAD'].astype(str) + ". ", "") 

        )
    )
    df['mensaje'] = mensaje
    df['objetivo'] = "2.1"
    
    df_failures = df[df['fail_count'] > 0]
    return df_failures[['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']]








