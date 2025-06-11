from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from typing import List, Dict
from datetime import datetime

import re

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def validation_indisponibilidad(df_merged: DataFrame) -> DataFrame:
    """
    Validate anexos indisponibilidad.
    Checks if the INDISPONIBILIDAD column matches the expected format with:
    - Header (Se tuvo indisponibilidad...)
    - Periods (DD/MM/YYYY hh:mm ... hasta el día ...)
    - Total (Total de horas... HH:MM horas)
    
    Parameters
    ----------
    df_merged : pyspark.sql.DataFrame
        DataFrame containing at least the columns:
        - INDISPONIBILIDAD (string)
        - clock_stops_paragraph_header (string)
        - clock_stops_paragraph_periodos (string)
        - clock_stops_paragraph_footer (string)

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame with these additional columns:
        - indisponibilidad_header (string): Extracted header
        - indisponibilidad_periodos (string): Extracted periods
        - indisponibilidad_total (string): Extracted total
        - indisponibilidad_header_match (boolean): True if header matches
        - indisponibilidad_periodos_match (boolean): True if periods match
        - indisponibilidad_total_match (boolean): True if total matches
        - Validation_OK (boolean): True if all validations pass
        - fail_count (integer): Number of failed validations
    """
    # Cache the DataFrame since it will be used multiple times
    df = df_merged.cache()

    # Define regex patterns
    header_pattern = r"^Se tuvo indisponibilidad por parte del cliente.*"
    periodo_pattern = r"^\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}.*hasta el día\s+\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}.*$"
    total_pattern = r"^\(Total de horas sin acceso a la sede:\s*(\d{1,3}:\d{2})\s*horas\)"

    # Define UDFs for text processing
    def split_indispo(text: str) -> Dict[str, str]:
        if not text:
            return {
                "indisponibilidad_header": "",
                "indisponibilidad_periodos": "",
                "indisponibilidad_total": ""
            }

        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        
        # Extract header
        header = ""
        for ln in lines:
            if re.match(header_pattern, ln, re.IGNORECASE):
                header = ln
                break
        
        # Extract periods
        periodos = [ln for ln in lines if re.match(periodo_pattern, ln, re.IGNORECASE)]
        periodos_fill_ceros = []
        for p in periodos:
            # Pad days and hours with zeros
            p = re.sub(r"^(\d)(?=/)", lambda m: m.group(1).zfill(2), p)
            p = re.sub(r"(?<=hasta el día\s)(\d)(?=/)", lambda m: m.group(1).zfill(2), p)
            p = re.sub(r"\b(\d)(?=:\d{2}(?::\d{2})?)", lambda m: m.group(1).zfill(2), p)
            periodos_fill_ceros.append(p)
        
        # Extract total
        total = ""
        for ln in lines:
            m = re.match(total_pattern, ln, re.IGNORECASE)
            if m:
                line = re.sub(r"\b(\d)(?=:\d{2}(?::\d{2})?)", lambda m: m.group(1).zfill(2), ln)
                m = re.match(total_pattern, line, re.IGNORECASE)
                total = m.group(1)
                break

        return {
            "indisponibilidad_header": header,
            "indisponibilidad_periodos": "\n".join(periodos_fill_ceros),
            "indisponibilidad_total": total
        }

    # Register UDF
    split_indispo_udf = F.udf(split_indispo, MapType(StringType(), StringType()))

    # Apply UDF to split INDISPONIBILIDAD into components
    df = df.withColumn(
        "indisponibilidad_components",
        split_indispo_udf(F.col("INDISPONIBILIDAD"))
    )

    # Extract components into separate columns
    df = df.withColumn(
        "indisponibilidad_header",
        F.col("indisponibilidad_components").getItem("indisponibilidad_header")
    ).withColumn(
        "indisponibilidad_periodos",
        F.col("indisponibilidad_components").getItem("indisponibilidad_periodos")
    ).withColumn(
        "indisponibilidad_total",
        F.col("indisponibilidad_components").getItem("indisponibilidad_total")
    )

    # Check if components match expected values
    df = df.withColumn(
        "indisponibilidad_header_match",
        F.trim(F.col("indisponibilidad_header")) == F.trim(F.col("clock_stops_paragraph_header"))
    ).withColumn(
        "indisponibilidad_periodos_match",
        F.trim(F.col("indisponibilidad_periodos")) == F.trim(F.col("clock_stops_paragraph_periodos"))
    ).withColumn(
        "indisponibilidad_total_match",
        F.trim(F.col("indisponibilidad_total")) == F.trim(F.col("clock_stops_paragraph_footer"))
    )

    # Calculate overall validation status
    df = df.withColumn(
        "Validation_OK",
        F.col("indisponibilidad_header_match") &
        F.col("indisponibilidad_periodos_match") &
        F.col("indisponibilidad_total_match")
    )

    # Count failures
    df = df.withColumn(
        "fail_count",
        F.when(~F.col("indisponibilidad_header_match"), 1).otherwise(0) +
        F.when(~F.col("indisponibilidad_periodos_match"), 1).otherwise(0) +
        F.when(~F.col("indisponibilidad_total_match"), 1).otherwise(0)
    )

    return df

@log_exceptions
def build_failure_messages_indisponibilidad(df: DataFrame) -> DataFrame:
    """
    Builds a descriptive message for the 'INDISPONIBILIDAD' validation.
    Returns rows that fail any check (fail_count > 0) with columns:
    -'nro_incidencia'
    - 'mensaje'
    - 'TIPO REPORTE'
    - 'objetivo'
    """
    # Build error message using PySpark's concat and when functions
    df = df.withColumn(
        "mensaje",
        F.when(
            F.col("Validation_OK"),
            F.lit("\n\n Validación exitosa: INDISPONIBILIDAD coincide con las paradas de reloj")
        ).otherwise(
            F.concat(
                F.when(
                    ~F.col("indisponibilidad_header_match"),
                    F.concat(
                        F.lit("\n Encabezado inválido en EXCEL-CORTE columna INDISPONIBILIDAD: \n"),
                        F.col("indisponibilidad_header"),
                        F.lit("\n es diferente a formato de Encabezado Indisponibilidad debe ser: \n"),
                        F.col("clock_stops_paragraph_header")
                    )
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col("indisponibilidad_periodos_match"),
                    F.concat(
                        F.lit("\n Parada(s) de clientes en  CORTE - EXCEL columna INDISPONIBILIDAD : \n"),
                        F.col("indisponibilidad_periodos"),
                        F.lit("\n  ES DIFERENTE A SGA PAUSA CLIENTE SIN OVERLAP: \n"),
                        F.col("clock_stops_paragraph_periodos")
                    )
                ).otherwise(F.lit("")),
                F.when(
                    ~F.col("indisponibilidad_total_match"),
                    F.concat(
                        F.lit("\n Total inválido CORTE - EXCEL columna INDISPONIBILIDAD: \n"),
                        F.col("indisponibilidad_total"),
                        F.lit("\n  ES DIFERENTE a total SGA PAUSA CLIENTE SIN OVERLAP: \n"),
                        F.col("clock_stops_paragraph_footer")
                    )
                ).otherwise(F.lit(""))
            )
        )
    ).withColumn(
        "objetivo",
        F.lit("1.11")
    )

    # Filter failures and select required columns
    return df.filter(F.col("fail_count") > 0).select(
        "nro_incidencia",
        "mensaje",
        "TIPO REPORTE",
        "objetivo"
    )



