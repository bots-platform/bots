from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict
from datetime import datetime

from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def validation_averia_tipificacion_problema(df_merged: DataFrame) -> DataFrame:
    """
    Validates the 'AVERIA' and 'TIPIFICACION DEL PROBLEMA' columns in CORTE-EXCEL by comparing:
    - The 'AVERIA' value in CORTE-EXCEL
    vs.
    - The expected 'AVERIA' value based on 'TIPIFICACION DEL PROBLEMA' column.
    
    Parameters
    ----------
    df_merged : pyspark.sql.DataFrame
        DataFrame containing at least the columns:
        - AVERIA_trimed (string)
        - TIPIFICACION DEL PROBLEMA_trimed (string)

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame with these additional columns:
        - non_empty_averia (boolean): True if AVERIA is not null
        - non_empty_tipificacion (boolean): True if TIPIFICACION DEL PROBLEMA is not null
        - match_averia (boolean): True if AVERIA matches expected value
        - Validation_OK (boolean): True if all validations pass
        - fail_count (integer): Number of failed validations (0-3)
    """
    with spark_manager.get_session():
      
        df = df_merged.cache()
        
        df = df.withColumn('non_empty_averia', F.col('AVERIA_trimed').isNotNull()) \
              .withColumn('non_empty_tipificacion', F.col('TIPIFICACION DEL PROBLEMA_trimed').isNotNull())

        df = df.withColumn(
            'match_averia',
            (
                (F.col('AVERIA_trimed') == F.lit('SI')) & (F.col('TIPIFICACION DEL PROBLEMA_trimed') == F.lit('AVERIA')) |
                (F.col('AVERIA_trimed') == F.lit('NO')) & (F.col('TIPIFICACION DEL PROBLEMA_trimed') != F.lit('AVERIA'))
            )
        )

        df = df.withColumn(
            'Validation_OK',
            F.col('non_empty_averia') &
            F.col('non_empty_tipificacion') &
            F.col('match_averia')
        )

        df = df.withColumn(
            'fail_count',
            F.when(~F.col('non_empty_averia'), 1).otherwise(0) +
            F.when(~F.col('non_empty_tipificacion'), 1).otherwise(0) +
            F.when(~F.col('match_averia'), 1).otherwise(0)
        )

        return df

@log_exceptions
def build_failure_messages_averia_tipificacion_problema(df: DataFrame) -> DataFrame:
    """
    Builds a descriptive message for the 'AVERIA' and 'TIPIFICACION DEL PROBLEMA' validation.
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
                F.lit("Validation de AVERIA y TIPIFICACION DEL PROBLEMA exitosa")
            ).otherwise(
                F.concat(
                    F.when(
                        ~F.col('non_empty_averia'),
                        F.lit("\n  AVERIA en CORTE-EXCEL es vacio. \n")
                    ).otherwise(F.lit("")),
                    F.when(
                        ~F.col('non_empty_tipificacion'),
                        F.lit("\n  TIPIFICACION DEL PROBLEMA en CORTE-EXCEL es vacio. \n")
                    ).otherwise(F.lit("")),
                    F.when(
                        ~F.col('match_averia'),
                        F.concat(
                            F.lit("\n  AVERIA en CORTE-EXCEL: \n"),
                            F.col('AVERIA_trimed'),
                            F.lit("\n no coincide con el valor esperado basado en TIPIFICACION DEL PROBLEMA: \n"),
                            F.col('TIPIFICACION DEL PROBLEMA_trimed')
                        )
                    ).otherwise(F.lit(""))
                )
            )
        ).withColumn(
            'objetivo',
            F.lit("1.6")
        )

        return df.filter(F.col('fail_count') > 0).select(
            'nro_incidencia',
            'mensaje',
            'TIPO REPORTE',
            'objetivo'
        )

