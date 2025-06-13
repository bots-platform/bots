from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, expr, array, struct, udf
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, ArrayType
from datetime import datetime, timedelta
import pandas as pd

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

from app.modules.sga.minpub.report_validator.service.objetivos.utils.formattings import (
    make_paragraph_paradas_cliente, make_paragraph_paradas_cliente_total, make_paragraph_paradas_cliente_header,
    make_paragraph_paradas_cliente_periodos, make_paragraph_paradas_cliente_footer
)

from app.modules.sga.minpub.report_validator.service.objetivos.utils.calculations import (
    calculate_total_clock_stop_minutes_by_incidencia,
    calculate_total_clock_stop_by_incidencia
)

from app.modules.sga.minpub.report_validator.service.objetivos.utils.spark_manager import spark_manager

@log_exceptions
def merge_sga_335_corte_excel_sharepoint_cuismp_sga380(
        df_corte_excel: DataFrame,
        df_sga_dinamico_335: DataFrame,
        df_cid_cuismp_sharepoint: DataFrame,
        df_sga_dinamico_380: DataFrame,
        match_type: str
    ) -> pd.DataFrame:
    """
    Common merge function for Objective 1 using PySpark.

    Merges:
      - corte-excel with sga_dinamico_335 on 'nro_incidencia'
      - Result with df_cid_cuismp_sharepoint on 'cid'

    Returns a merged DataFrame with common columns needed.
    """
    with spark_manager.get_session_context() as spark:
        try:
            # First merge: corte-excel with sga_dinamico_335
            df_merged_sga335_excel = df_corte_excel.join(
                df_sga_dinamico_335,
                on='nro_incidencia',
                how='left'
            ).withColumn(
                '_merge',
                when(col('nro_incidencia').isNotNull(), 'both').otherwise('left_only')
            )

            # Second merge: with sharepoint data
            df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp = df_merged_sga335_excel.join(
                df_cid_cuismp_sharepoint,
                on='cid',
                how='left'
            )

            # Calculate sum_paradas using UDF for complex calculations
            @udf(DoubleType())
            def calculate_sum_paradas(nro_incidencia, interruption_start, interruption_end):
                return calculate_total_clock_stop_minutes_by_incidencia(
                    nro_incidencia=nro_incidencia,
                    interruption_start=interruption_start,
                    interruption_end=interruption_end,
                    df_sga_paradas=df_sga_dinamico_380
                )

            # Calculate clock_stops using UDF
            @udf(ArrayType(StructType([
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("duration", DoubleType(), True)
            ])))
            def calculate_clock_stops(nro_incidencia, interruption_start, interruption_end):
                return calculate_total_clock_stop_by_incidencia(
                    nro_incidencia=nro_incidencia,
                    interruption_start=interruption_start,
                    interruption_end=interruption_end,
                    df_sga_paradas=df_sga_dinamico_380
                )

            # Apply calculations
            df_with_calculations = df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp.withColumn(
                'sum_paradas',
                calculate_sum_paradas(col('nro_incidencia'), col('interrupcion_inicio'), col('interrupcion_fin'))
            ).withColumn(
                'clock_stops',
                calculate_clock_stops(col('nro_incidencia'), col('interrupcion_inicio'), col('interrupcion_fin'))
            )

            # Apply formatting functions using UDFs
            @udf(StringType())
            def format_clock_stops_paragraph(clock_stops):
                return make_paragraph_paradas_cliente(clock_stops)

            @udf(StringType())
            def format_clock_stops_header(clock_stops):
                return make_paragraph_paradas_cliente_header(clock_stops)

            @udf(StringType())
            def format_clock_stops_periodos(clock_stops):
                return make_paragraph_paradas_cliente_periodos(clock_stops)

            @udf(StringType())
            def format_clock_stops_footer(clock_stops):
                return make_paragraph_paradas_cliente_total(clock_stops)

            # Add formatted columns
            df_final = df_with_calculations.withColumn(
                'clock_stops_paragraph',
                format_clock_stops_paragraph(col('clock_stops'))
            ).withColumn(
                'clock_stops_paragraph_header',
                format_clock_stops_header(col('clock_stops'))
            ).withColumn(
                'clock_stops_paragraph_periodos',
                format_clock_stops_periodos(col('clock_stops'))
            ).withColumn(
                'clock_stops_paragraph_footer',
                format_clock_stops_footer(col('clock_stops'))
            )

            matched_rows = df_final.filter(col('_merge') == match_type)

            pdf = matched_rows.toPandas()
            return pdf

        except Exception as e:
            raise Exception(f"Error merging SGA data: {str(e)}")
        finally:
            # Cleanup any cached DataFrames if needed
            if 'df_merged_sga335_excel' in locals():
                df_merged_sga335_excel.unpersist()
            if 'df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp' in locals():
                df_merge_sga_335_corte_excel_with_sharepoint_cid_cuismp.unpersist()
            if 'df_with_calculations' in locals():
                df_with_calculations.unpersist()
  