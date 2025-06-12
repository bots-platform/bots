from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType
from typing import List, Dict

from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def validate_anexos_indisponibilidad_word(df_merged: DataFrame) -> DataFrame:
    """
    Validate anexos indisponibilidad 
    Return a DataFrame with new Boolean columns for validation
    """
    with spark_manager.get_session():
        df = df_merged.cache()

        df = df.filter(
            (F.col('TIPO REPORTE') == 'RECLAMO') &
            (F.col('clock_stops_paragraph_header') != "")
        )

        df = df.withColumn(
            'indisponibilidad_header_match',
            F.trim(F.col('indisponibilidad_header').cast(StringType())) == 
            F.col('clock_stops_paragraph_header')
        )

        df = df.withColumn(
            'indisponibilidad_periodos_match',
            F.trim(F.col('indisponibilidad_periodos').cast(StringType())) == 
            F.col('clock_stops_paragraph_periodos')
        )

        df = df.withColumn(
            'indisponibilidad_total_match',
            F.trim(F.col('indisponibilidad_total').cast(StringType())) == 
            F.col('clock_stops_paragraph_footer')
        )

        df = df.withColumn(
            'Validation_OK',
            F.col('indisponibilidad_header_match') &
            F.col('indisponibilidad_periodos_match') &
            F.col('indisponibilidad_total_match')
        )

        df = df.withColumn(
            'fail_count',
            F.when(~F.col('indisponibilidad_header_match'), 1).otherwise(0) +
            F.when(~F.col('indisponibilidad_periodos_match'), 1).otherwise(0) +
            F.when(~F.col('indisponibilidad_total_match'), 1).otherwise(0)
        )

        return df

@log_exceptions
def build_failure_messages_validate_anexos_indisponibilidad_word(df: DataFrame) -> DataFrame:
    """
    Returns a DataFrame of failures with columns:
    ['nro_incidencia', 'mensaje', 'TIPO REPORTE', 'objetivo']
    """
    with spark_manager.get_session():
        if df is None or df.isEmpty() or 'Validation_OK' not in df.columns:
            return df.sparkSession.createDataFrame(
                [], 
                ['nro_incidencia', 'mensaje', 'TIPO REPORTE', 'objetivo']
            )

        df = df.withColumn(
            'mensaje',
            F.when(
                F.col('Validation_OK'),
                F.lit("Validación exitosa: ANEXOS INDISPONIBILIDAD coincide con las paradas de reloj")
            ).otherwise(
                F.concat(
                    F.when(
                        ~F.col('indisponibilidad_header_match'),
                        F.concat(
                            F.lit("\n No coincide texto inicio de WORD ANEXOS INDISPONIBILIDAD : \n"),
                            F.col('indisponibilidad_header'),
                            F.lit("\n es diferente a SGA PAUSA CLIENTE SIN OVERLAP: \n"),
                            F.col('clock_stops_paragraph_header')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('indisponibilidad_periodos_match'),
                        F.concat(
                            F.lit("\n No coincide paradas de reloj de WORD ANEXOS INDISPONIBILIDAD : \n"),
                            F.col('indisponibilidad_periodos'),
                            F.lit("\n es diferente a SGA PAUSA CLIENTE SIN OVERLAP :  \n"),
                            F.col('clock_stops_paragraph_periodos')
                        )
                    ).otherwise(F.lit("")),
                    
                    F.when(
                        ~F.col('indisponibilidad_total_match'),
                        F.concat(
                            F.lit("\n No coincide total horas sin acceso a la sede de WORD ANEXOS INDISPONIBILIDAD : \n "),
                            F.col('indisponibilidad_total'),
                            F.lit("\n es diferente a SGA PAUSA CLIENTE SIN OVERLAP :  \n"),
                            F.col('clock_stops_paragraph_footer')
                        )
                    ).otherwise(F.lit(""))
                )
            )
        ).withColumn(
            'objetivo',
            F.lit("3.1")
        )

        return df.filter(F.col('fail_count') > 0).select(
            'nro_incidencia',
            'mensaje',
            'TIPO REPORTE',
            'objetivo'
        )
