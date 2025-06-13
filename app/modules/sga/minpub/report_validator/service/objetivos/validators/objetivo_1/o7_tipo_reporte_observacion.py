from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict
from datetime import datetime
import pandas as pd

from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def validation_tipo_reporte_observacion(df_merged: DataFrame) -> pd.DataFrame:
    """
    Validates the 'TIPO REPORTE' and 'OBSERVACION' columns in CORTE-EXCEL by comparing:
    - The 'TIPO REPORTE' value in CORTE-EXCEL
    vs.
    - The expected 'TIPO REPORTE' value based on 'OBSERVACION' column.
    
    Parameters
    ----------
    df_merged : pyspark.sql.DataFrame
        DataFrame containing at least the columns:
        - TIPO REPORTE_trimed (string)
        - OBSERVACION_trimed (string)

    Returns
    -------
    pandas.DataFrame
        DataFrame with these additional columns:
        - non_empty_tipo_reporte (boolean): True if TIPO REPORTE is not null
        - non_empty_observacion (boolean): True if OBSERVACION is not null
        - match_tipo_reporte (boolean): True if TIPO REPORTE matches expected value
        - Validation_OK (boolean): True if all validations pass
        - fail_count (integer): Number of failed validations (0-3)
    """
    with spark_manager.get_session_context() as spark:
        df = df_merged.cache()
        
        df = df.withColumn('non_empty_tipo_reporte', F.col('TIPO REPORTE_trimed').isNotNull()) \
              .withColumn('non_empty_observacion', F.col('OBSERVACION_trimed').isNotNull())

        df = df.withColumn(
            'match_tipo_reporte',
            (
                (F.col('TIPO REPORTE_trimed') == F.lit('INCIDENTE')) & (F.col('OBSERVACION_trimed') == F.lit('INCIDENTE')) |
                (F.col('TIPO REPORTE_trimed') == F.lit('INTERRUPCION')) & (F.col('OBSERVACION_trimed') == F.lit('INTERRUPCION'))
            )
        )

        df = df.withColumn(
            'Validation_OK',
            F.col('non_empty_tipo_reporte') &
            F.col('non_empty_observacion') &
            F.col('match_tipo_reporte')
        )

        df = df.withColumn(
            'fail_count',
            F.when(~F.col('non_empty_tipo_reporte'), 1).otherwise(0) +
            F.when(~F.col('non_empty_observacion'), 1).otherwise(0) +
            F.when(~F.col('match_tipo_reporte'), 1).otherwise(0)
        )

        pdf = df.toPandas()
        return pdf

@log_exceptions
def build_failure_messages_tipo_reporte_observacion(df: DataFrame) -> pd.DataFrame:
    """
    Builds a descriptive message for the 'TIPO REPORTE' and 'OBSERVACION' validation.
    Returns rows that fail any check (fail_count > 0) with columns:
    -'nro_incidencia'
    - 'mensaje'
    - 'TIPO REPORTE'
    - 'objetivo'
    """
    with spark_manager.get_session_context() as spark:
        df = df.withColumn(
            'mensaje',
            F.when(
                F.col('Validation_OK'),
                F.lit("Validation de TIPO REPORTE y OBSERVACION exitosa")
            ).otherwise(
                F.concat(
                    F.when(
                        ~F.col('non_empty_tipo_reporte'),
                        F.lit("\n  TIPO REPORTE en CORTE-EXCEL es vacio. \n")
                    ).otherwise(F.lit("")),
                    F.when(
                        ~F.col('non_empty_observacion'),
                        F.lit("\n  OBSERVACION en CORTE-EXCEL es vacio. \n")
                    ).otherwise(F.lit("")),
                    F.when(
                        ~F.col('match_tipo_reporte'),
                        F.concat(
                            F.lit("\n  TIPO REPORTE en CORTE-EXCEL: \n"),
                            F.col('TIPO REPORTE_trimed'),
                            F.lit("\n no coincide con el valor esperado basado en OBSERVACION: \n"),
                            F.col('OBSERVACION_trimed')
                        )
                    ).otherwise(F.lit(""))
                )
            )
        ).withColumn(
            'objetivo',
            F.lit("1.7")
        )
        df_failures = df.filter(F.col('fail_count') > 0).select(
            'nro_incidencia',
            'mensaje',
            'TIPO REPORTE',
            'objetivo'
        )
        pdf = df_failures.toPandas()
        return pdf
  