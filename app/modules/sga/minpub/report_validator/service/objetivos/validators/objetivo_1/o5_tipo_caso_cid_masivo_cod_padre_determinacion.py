from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict
from datetime import datetime

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
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
    
    cleaned = pattern.sub("", text).strip()
    return cleaned

@log_exceptions
def validation_tipo_caso_cid_masivo_cod_padre_determinacion(df_merged: DataFrame) -> DataFrame:
    """
    Validates the 'TIPO CASO' column in CORTE-EXCEL by comparing:
    - The 'TIPO CASO' value in CORTE-EXCEL
    vs.
    - The expected 'TIPO CASO' value based on 'CID' and 'MASIVO' columns.
    
    Parameters
    ----------
    df_merged : pyspark.sql.DataFrame
        DataFrame containing at least the columns:
        - TIPO CASO_trimed (string)
        - CID_trimed (string)
        - MASIVO_trimed (string)

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame with these additional columns:
        - non_empty_tipo_caso (boolean): True if TIPO CASO is not null
        - non_empty_cid (boolean): True if CID is not null
        - non_empty_masivo (boolean): True if MASIVO is not null
        - match_tipo_caso (boolean): True if TIPO CASO matches expected value
        - Validation_OK (boolean): True if all validations pass
        - fail_count (integer): Number of failed validations (0-4)
    """
    # Cache the DataFrame since it will be used multiple times
    df = df_merged.cache()
    
    # Check if columns are non-empty
    df = df.withColumn('non_empty_tipo_caso', F.col('TIPO CASO_trimed').isNotNull()) \
          .withColumn('non_empty_cid', F.col('CID_trimed').isNotNull()) \
          .withColumn('non_empty_masivo', F.col('MASIVO_trimed').isNotNull())

    # Check if TIPO CASO matches expected value based on MASIVO
    df = df.withColumn(
        'match_tipo_caso',
        (
            (F.col('TIPO CASO_trimed') == F.lit('MASIVO')) & (F.col('MASIVO_trimed') == F.lit('SI')) |
            (F.col('TIPO CASO_trimed') == F.lit('INDIVIDUAL')) & (F.col('MASIVO_trimed') == F.lit('NO'))
        )
    )

    # Calculate overall validation status
    df = df.withColumn(
        'Validation_OK',
        F.col('non_empty_tipo_caso') &
        F.col('non_empty_cid') &
        F.col('non_empty_masivo') &
        F.col('match_tipo_caso')
    )

    # Count failures
    df = df.withColumn(
        'fail_count',
        F.when(~F.col('non_empty_tipo_caso'), 1).otherwise(0) +
        F.when(~F.col('non_empty_cid'), 1).otherwise(0) +
        F.when(~F.col('non_empty_masivo'), 1).otherwise(0) +
        F.when(~F.col('match_tipo_caso'), 1).otherwise(0)
    )

    return df

@log_exceptions
def build_failure_messages_tipo_caso_cid_masivo_cod_padre_determinacion(df: DataFrame) -> DataFrame:
    """
    Builds a descriptive message for the 'TIPO CASO' validation.
    Returns rows that fail any check (fail_count > 0) with columns:
    -'nro_incidencia'
    - 'mensaje'
    - 'TIPO REPORTE'
    - 'objetivo'
    """
    # Build error message using PySpark's concat and when functions
    df = df.withColumn(
        'mensaje',
        F.when(
            F.col('Validation_OK'),
            F.lit("Validation de TIPO CASO exitosa")
        ).otherwise(
            F.concat(
                F.when(
                    ~F.col('non_empty_tipo_caso'),
                    F.lit("\n  TIPO CASO en CORTE-EXCEL es vacio. \n")
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col('non_empty_cid'),
                    F.lit("\n  CID en CORTE-EXCEL es vacio. \n")
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col('non_empty_masivo'),
                    F.lit("\n  MASIVO en CORTE-EXCEL es vacio. \n")
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col('match_tipo_caso'),
                    F.concat(
                        F.lit("\n  TIPO CASO en CORTE-EXCEL: \n"),
                        F.col('TIPO CASO_trimed'),
                        F.lit("\n no coincide con el valor esperado basado en MASIVO: \n"),
                        F.col('MASIVO_trimed')
                    )
                ).otherwise(F.lit(""))
            )
        )
    ).withColumn(
        'objetivo',
        F.lit("1.5")
    )

    # Filter failures and select required columns
    return df.filter(F.col('fail_count') > 0).select(
        'nro_incidencia',
        'mensaje',
        'TIPO REPORTE',
        'objetivo'
    )

