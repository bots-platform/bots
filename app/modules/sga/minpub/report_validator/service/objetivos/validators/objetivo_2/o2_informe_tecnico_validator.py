from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from typing import List, Dict
from datetime import datetime

from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)
from app.modules.sga.minpub.report_validator.service.objetivos.utils.etl_utils import normalize_text

@log_exceptions
def validate_informe_tecnico_word(df_merged: DataFrame, componente_word: str) -> DataFrame:
    """
    Validate values columns coming from word and excel files
    Return a DataFrame with new Boolean columns for validation
    
    Columnas en EXCEL	                                        Columnas en WORD DATOS
    "FECHA Y HORA INICIO",                                      "Fecha y hora inicio":   
    "FECHA Y HORA FIN",                                         "Fecha y hora fin": 
    "CUISMP",                                                   "CUISMP": 
    "TIPO CASO",                                                "Tipo Caso":                    
    "OBSERVACION",                                              "Observación": 
    "DETERMINACIÓN DE LA CAUSA",                                "DETERMINACIÓN DE LA CAUSA":  
    "MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS",              "MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS": 
    """
    with spark_manager.get_session():
        # Cache the DataFrame since it will be used multiple times
        df = df_merged.cache()

        # Add CUISMP column based on componente
        df = df.withColumn(
            'CUISMP_word',
            F.when(
                F.lit(componente_word) == 'COMPONENTE II',
                F.col('CUISMP_word_datos_informe')
            ).when(
                F.lit(componente_word) == 'COMPONENTE IV',
                F.col('CUISMP_word_telefonia_informe')
            )
        )

        # Add MEDIDAS CORRECTIVAS column based on componente
        df = df.withColumn(
            'MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word',
            F.when(
                F.lit(componente_word) == 'COMPONENTE II',
                F.col('MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word_datos_informe')
            ).when(
                F.lit(componente_word) == 'COMPONENTE IV',
                F.col('MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word_telefonia_informe')
            )
        )

        # Add DETERMINACIÓN DE LA CAUSA column based on componente
        df = df.withColumn(
            'DETERMINACIÓN DE LA CAUSA_word',
            F.when(
                F.lit(componente_word) == 'COMPONENTE II',
                F.col('DETERMINACIÓN DE LA CAUSA_word_datos_informe')
            ).when(
                F.lit(componente_word) == 'COMPONENTE IV',
                F.col('DETERMINACIÓN DE LA CAUSA_word_telefonia_informe')
            )
        )

        # Add validation columns
        df = df.withColumn(
            'Fecha_hora_inicio_match',
            F.col('FECHA_Y_HORA_INICIO_fmt') == F.col('Fecha y hora inicio')
        )

        df = df.withColumn(
            'fecha_hora_fin_match',
            F.col('FECHA_Y_HORA_FIN_fmt') == F.col('Fecha y hora fin')
        )

        df = df.withColumn(
            'CUISMP_match',
            F.col('CUISMP_corte_excel') == F.col('CUISMP_word')
        )

        df = df.withColumn(
            'tipo_caso_match',
            F.col('TIPO CASO') == F.col('Tipo Caso')
        )

        df = df.withColumn(
            'observacion_match',
            F.col('OBSERVACIÓN') == F.col('Observación')
        )

        df = df.withColumn(
            'dt_causa_match',
            F.col('DETERMINACIÓN DE LA CAUSA_corte_excel') == F.col('DETERMINACIÓN DE LA CAUSA_word')
        )

        # Create UDF for normalize_text function
        normalize_text_udf = F.udf(normalize_text, StringType())

        # Add medidas_correctivas_match using UDF
        df = df.withColumn(
            'medidas_correctivas_match',
            normalize_text_udf(F.col('MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_corte_excel')) ==
            normalize_text_udf(F.col('MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word'))
        )

        df = df.withColumn(
            'Validation_OK',
            F.col('Fecha_hora_inicio_match') &
            F.col('fecha_hora_fin_match') &
            F.col('CUISMP_match') &
            F.col('tipo_caso_match') &
            F.col('observacion_match') &
            F.col('dt_causa_match') &
            F.col('medidas_correctivas_match')
        )

        # Count failures
        df = df.withColumn(
            'fail_count',
            F.when(~F.col('Fecha_hora_inicio_match'), 1).otherwise(0) +
            F.when(~F.col('fecha_hora_fin_match'), 1).otherwise(0) +
            F.when(~F.col('CUISMP_match'), 1).otherwise(0) +
            F.when(~F.col('tipo_caso_match'), 1).otherwise(0) +
            F.when(~F.col('observacion_match'), 1).otherwise(0) +
            F.when(~F.col('dt_causa_match'), 1).otherwise(0) +
            F.when(~F.col('medidas_correctivas_match'), 1).otherwise(0)
        )

        return df

@log_exceptions
def build_failure_messages_validate_informe_tecnico_word(df: DataFrame) -> DataFrame:
    """
    Builds the 'mensaje' column using PySpark operations.
    Adds the 'objetivo' column (constant value of 2.2) and filters
    rows that fail at least one validation.
    
    Returns a DataFrame with:
      ['nro_incidencia', 'mensaje', 'TIPO REPORTE', 'objetivo']
    """
    with spark_manager.get_session():
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
                            F.lit("\n No coincide Fecha y hora inicio de WORD informe técnico : \n"),
                            F.col('Fecha y hora inicio'),
                            F.lit("\n es diferente a EXCEL-CORTE:  \n\n"),
                            F.col('FECHA_Y_HORA_INICIO_fmt')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('fecha_hora_fin_match'),
                        F.concat(
                            F.lit("\n  No coincide Fecha y hora fin de WORD en informe técnico : \n"),
                            F.col('Fecha y hora fin'),
                            F.lit("\n es diferente a EXCEL-CORTE:  \n"),
                            F.col('FECHA_Y_HORA_FIN_fmt')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('CUISMP_match'),
                        F.concat(
                            F.lit("\n  No coincide CUISMP de WORD informe técnico : \n"),
                            F.col('CUISMP_word'),
                            F.lit("\n es diferente a CUISMP en EXCEL-CORTE: \n"),
                            F.col('CUISMP_corte_excel')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('tipo_caso_match'),
                        F.concat(
                            F.lit("\n\n  No coincide Avería reportada de WORD informe técnico : \n\n"),
                            F.col('Tipo Caso'),
                            F.lit("\n\n es diferente a TIPO CASO en EXCEL-CORTE: \n\n"),
                            F.col('TIPO CASO')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('observacion_match'),
                        F.concat(
                            F.lit("\n  No coincide Observacion de WORD informe técnico : \n"),
                            F.col('Observación'),
                            F.lit("\n es diferente a OBSERVACIÓN en EXCEL-CORTE: \n"),
                            F.col('OBSERVACIÓN')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('dt_causa_match'),
                        F.concat(
                            F.lit("\n  No coincide Determinación de la causa de WORD informe técnico : \n"),
                            F.col('DETERMINACIÓN DE LA CAUSA_word'),
                            F.lit("\n es diferente a DETERMINACION DE LA CAUSA en EXCEL-CORTE: \n"),
                            F.col('DETERMINACIÓN DE LA CAUSA_corte_excel')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('medidas_correctivas_match'),
                        F.concat(
                            F.lit("\n  No coincide MEDIDAS CORRECTIVAS de WORD informe técnico : \n"),
                            F.col('MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_word'),
                            F.lit("\n es diferente a MEDIDAS CORRECTIVAS en EXCEL-CORTE: \n"),
                            F.col('MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS_corte_excel')
                        )
                    ).otherwise(F.lit(""))
                )
            )
        ).withColumn(
            'objetivo',
            F.lit("2.2")
        )

        return df.filter(F.col('fail_count') > 0).select(
            'nro_incidencia',
            'mensaje',
            'TIPO REPORTE',
            'objetivo'
        )


