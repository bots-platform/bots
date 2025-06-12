from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict
from datetime import datetime

import re

from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def validation_responsable(df_merged: DataFrame) -> DataFrame:
    """
    Validates the 'RESPONSABLE' column in CORTE-EXCEL by checking:
    - If the value is non-empty
    - If the value matches the expected format
    
    Parameters
    ----------
    df_merged : pyspark.sql.DataFrame
        DataFrame containing at least the columns:
        - RESPONSABLE_trimed (string)

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame with these additional columns:
        - non_empty_responsable (boolean): True if RESPONSABLE is not null
        - match_responsable (boolean): True if RESPONSABLE matches expected values
        - Validation_OK (boolean): True if all validations pass
        - fail_count (integer): Number of failed validations (0-2)
    """
    with spark_manager.get_session():
        df = df_merged.cache()
        
        df = df.withColumn(
            'non_empty_responsable',
            F.col('RESPONSABLE_trimed').isNotNull()
        )

        df = df.withColumn(
            'match_responsable',
            F.col('RESPONSABLE_trimed').isin([
                'SOPORTE TÉCNICO',
                'SOPORTE TECNICO',
                'SOPORTE TÉCNICO - CLARO',
                'SOPORTE TECNICO - CLARO'
            ])
        )

        df = df.withColumn(
            'Validation_OK',
            F.col('non_empty_responsable') &
            F.col('match_responsable')
        )

        df = df.withColumn(
            'fail_count',
            F.when(~F.col('non_empty_responsable'), 1).otherwise(0) +
            F.when(~F.col('match_responsable'), 1).otherwise(0)
        )

        return df

@log_exceptions
def build_failure_messages_responsable(df: DataFrame) -> DataFrame:
    """
    Builds a descriptive message for the 'RESPONSABLE' validation.
    Returns rows that fail any check (fail_count > 0) with columns:
    -'nro_incidencia'
    - 'mensaje'
    - 'TIPO REPORTE'
    - 'objetivo'
    """
    with spark_manager.get_session():
        df = df.withColumn(
            'mensaje',
            F.when(
                F.col('Validation_OK'),
                F.lit("Validation de RESPONSABLE exitosa")
            ).otherwise(
                F.concat(
                    F.when(
                        ~F.col('non_empty_responsable'),
                        F.lit("\n  RESPONSABLE en CORTE-EXCEL es vacio. \n")
                    ).otherwise(F.lit("")),
                    F.when(
                        ~F.col('match_responsable'),
                        F.concat(
                            F.lit("\n  RESPONSABLE en CORTE-EXCEL: \n"),
                            F.col('RESPONSABLE_trimed'),
                            F.lit("\n no coincide con los valores permitidos: \n"),
                            F.lit("SOPORTE TÉCNICO, SOPORTE TECNICO, SOPORTE TÉCNICO - CLARO, SOPORTE TECNICO - CLARO")
                        )
                    ).otherwise(F.lit(""))
                )
            )
        ).withColumn(
            'objetivo',
            F.lit("1.9")
        )

        return df.filter(F.col('fail_count') > 0).select(
            'nro_incidencia',
            'mensaje',
            'TIPO REPORTE',
            'objetivo'
        )
