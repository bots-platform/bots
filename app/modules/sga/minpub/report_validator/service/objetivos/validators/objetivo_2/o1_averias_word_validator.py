# objetivo_2_validator.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict
from datetime import datetime

from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def validate_averias_word(df_merged: DataFrame, componente_word: str) -> DataFrame:
    """
    Validate values columns coming from word and excel files
    Return a DataFrame with new Boolean columns for validation
    
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
    with spark_manager.get_session():
        df = df_merged.cache()
        
        df = df.withColumn('Componente', F.lit(componente_word))

        # Determine CUISMP column name based on componente
        column_name_cuismp = F.when(
            F.lit(componente_word) == 'COMPONENTE II',
            'CUISMP_word_datos_averias'
        ).when(
            F.lit(componente_word) == 'COMPONENTE IV',
            'CUISMP_word_telefonia_averias'
        ).otherwise('')

        # Add CUISMP column
        df = df.withColumn('cuismp_word_averia', F.col(column_name_cuismp))

        # Add validation columns
        df = df.withColumn('Fecha_hora_inicio_match', 
                        F.col('FECHA Y HORA INICIO') == F.col('Fecha y Hora Inicio'))
        
        df = df.withColumn('fecha_hora_fin_match',
                        F.col('FECHA Y HORA FIN') == F.col('Fecha y Hora Fin'))
        
        df = df.withColumn('CUISMP_match',
                        F.col('CUISMP_corte_excel') == F.col('cuismp_word_averia'))
        
        df = df.withColumn('tipo_caso_match',
                        F.col('TIPO CASO') == F.col('Avería reportada'))
        
        df = df.withColumn('averia_match',
                        F.col('AVERÍA') == F.col('Causa'))
        
        df = df.withColumn('tiempo_hhmm_match',
                        F.col('TIEMPO (HH:MM)_trimed') == F.col('Tiempo real de afectación (HH:MM)'))
        
        df = df.withColumn('componente_match',
                        F.col('COMPONENTE') == F.col('Componente'))
        
        df = df.withColumn('df_match',
                        F.col('DF') == F.col('Distrito Fiscal'))
        
        df = df.withColumn('fin_inicio_hhmm_match',
                        F.col('FIN-INICIO (HH:MM)_trimed') == F.col('Tiempo Total (HH:MM)'))
        
        df = df.withColumn('responsabilidad_match',
                        F.col('RESPONSABILIDAD') == F.col('responsable'))

        # Calculate overall validation status
        df = df.withColumn('Validation_OK',
            F.col('Fecha_hora_inicio_match') &
            F.col('fecha_hora_fin_match') &
            F.col('CUISMP_match') &
            F.col('tipo_caso_match') &
            F.col('averia_match') &
            F.col('tiempo_hhmm_match') &
            F.col('componente_match') &
            F.col('df_match') &
            F.col('fin_inicio_hhmm_match') &
            F.col('responsabilidad_match')
        )

        # Count failures
        df = df.withColumn('fail_count',
            F.when(~F.col('Validation_OK'), 1).otherwise(0)
        )

        return df

@log_exceptions
def build_failure_messages_validate_averias_word(df: DataFrame) -> DataFrame:
    """
    Builds the 'mensaje' column using PySpark operations.
    Adds the 'objetivo' column (constant value of 2.1) and filters
    rows that fail at least one validation.
    
    Returns a DataFrame with:
      ['nro_incidencia', 'mensaje', 'TIPO REPORTE','objetivo']
    """
    with spark_manager.get_session():
        # Build error message using PySpark's concat and when functions
        df = df.withColumn(
            'mensaje',
            F.when(
                F.col('Validation_OK'),
                F.lit("Validation successful")
            ).otherwise(
                F.concat(
                    F.when(
                        ~F.col('Fecha_hora_inicio_match'),
                        F.concat(
                            F.lit("\n  No coincide Fecha y Hora Inicio de WORD CUADRO AVERIAS : \n"),
                            F.col('Fecha y Hora Inicio'),
                            F.lit("\n es diferente a FECHA Y HORA INICIO en EXCEL-CORTE:  \n"),
                            F.col('FECHA_Y_HORA_INICIO_fmt')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('fecha_hora_fin_match'),
                        F.concat(
                            F.lit("\n  No coincide Fecha y Hora Fin de WORD CUADRO AVERIAS : \n"),
                            F.col('Fecha y Hora Fin'),
                            F.lit("\n es diferente columna FECHA Y HORA FIN en EXCEL-CORTE:  \n"),
                            F.col('FECHA_Y_HORA_FIN_fmt')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('CUISMP_match'),
                        F.concat(
                            F.lit("\n  No coincide CUISMP en WORD CUADRO AVERIAS : \n"),
                            F.col('cuismp_word_averia'),
                            F.lit("\n es diferente a CUISMP en CORTE-EXCEL: \n"),
                            F.col('CUISMP_corte_excel')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('tipo_caso_match'),
                        F.concat(
                            F.lit("\n  No coincide Avería reportada de WORD CUADRO AVERIAS : \n"),
                            F.col('Avería reportada'),
                            F.lit("\n es diferente a TIPO CASO en EXCEL-CORTE: \n"),
                            F.col('TIPO CASO')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('averia_match'),
                        F.concat(
                            F.lit("\n  No coincide Causa : \n"),
                            F.col('Causa'),
                            F.lit("\n es diferente a AVERÍA de CORTE-EXCEL: \n"),
                            F.col('AVERÍA')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('tiempo_hhmm_match'),
                        F.concat(
                            F.lit("\n  No coincide Tiempo real de afectación de WORD CUADRO DE AVERIAS: \n"),
                            F.col('Tiempo real de afectación (HH:MM)'),
                            F.lit("\n es diferente a TIEMPO (HH:MM) de EXCEL-CORTE: \n"),
                            F.col('TIEMPO (HH:MM)_trimed')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('componente_match'),
                        F.concat(
                            F.lit(" \n No coincide COMPONENTE en  de WORD CUADRO DE AVERIAS : \n"),
                            F.col('Componente'),
                            F.lit("\n es diferente a la  columna COMPONENTE de CORTE-EXCEL: \n"),
                            F.col('COMPONENTE')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('df_match'),
                        F.concat(
                            F.lit("\n  No coincide Distrito Fiscal de WORD CUADRO DE AVERIAS : \n"),
                            F.col('Distrito Fiscal'),
                            F.lit("\n es diferente a column DF de CORTE-EXCEL: \n"),
                            F.col('DF')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('fin_inicio_hhmm_match'),
                        F.concat(
                            F.lit("\n No coincide Tiempo Total (HH:MM) de WORD CUADRO DE AVERIAS : \n"),
                            F.col('Tiempo Total (HH:MM)'),
                            F.lit("\n es diferente a la columna FIN-INICIO (HH:MM) de CORTE-EXCEL: \n"),
                            F.col('FIN-INICIO (HH:MM)_trimed')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('responsabilidad_match'),
                        F.concat(
                            F.lit("\n  No coincide Responsable de WORD CUADRO DE AVERIAS : \n"),
                            F.col('responsable'),
                            F.lit("\n es diferente a la columna RESPONSABILIDAD de CORTE-EXCEL: \n"),
                            F.col('RESPONSABILIDAD')
                        )
                    ).otherwise(F.lit(""))
                )
            )
        ).withColumn(
            'objetivo',
            F.lit("2.1")
        )

        # Filter failures and select required columns
        return df.filter(F.col('fail_count') > 0).select(
            'nro_incidencia',
            'mensaje',
            'TIPO REPORTE',
            'objetivo'
        )







