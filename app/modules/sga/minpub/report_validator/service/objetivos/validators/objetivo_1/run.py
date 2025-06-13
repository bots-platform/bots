from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import pandas as pd

from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_1.unmatched_messages import (
    build_message_merge_sga_335_corte_excel_unmatch
)

from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_1.o1_cuismp_distrito_fiscal_medidas import (
    validation_cuismp_distrito_fiscal_medidas,
    build_failure_messages_cuismp_distrito_fiscal_medidas
)

from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_1.o2_fecha_inicio_fin import (
    validation_fecha_inicio_fin,
    build_failure_messages_fechas_fin_inicio
)

from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_1.o3_fin_inicio_HHMM import (
    validation_fin_inicio_HHMM,
    build_failure_messages_diff_fin_inicio_HHMM
)

from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_1.o4_tiempo_HHMM_paradas_cliente import (
    validation_tiempo_HHMM_paradas_cliente,
    buid_failure_messages_tiempo_HHMM_paradas_cliente
)

from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_1.o5_tipo_caso_cid_masivo_cod_padre_determinacion import (
    validation_tipo_caso_cid_masivo_cod_padre_determinacion,
    build_failure_messages_tipo_caso_cid_masivo_cod_padre_determinacion
)

from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_1.o6_averia_tipificacion_problema import (
    validation_averia_tipificacion_problema,
    build_failure_messages_averia_tipificacion_problema
)

from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_1.o7_tipo_reporte_observacion import (
    validation_tipo_reporte_observacion,
    build_failure_messages_tipo_reporte_observacion
)

from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_1.o8_medidas_correctivas import (
    validation_medidas_correctivas,
    build_failure_messages_medidas_correctivas
)

from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_1.o9_responsable_validator import (
    validation_responsable,
    build_failure_messages_responsable
)

from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_1.o10_duracion_entero_validator import (
    validation_duracion_entero,
    build_failure_messages_duracion_entero
)

from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_1.o11_indisponibilidad_validator import (
    validation_indisponibilidad,
    build_failure_messages_indisponibilidad
)

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def run_objetivo_1(
    df_matched_corte_sga335_sharepoint_cuismp_sga380: DataFrame,
    df_unmatched_corte_sga335_sharepoint_cuismp_sga389: DataFrame
) -> pd.DataFrame:
    """
    Aggregates all sub-validations for Objective 1.
    Uses a common merge function and passes the merged data to each sub-validation.
    
    Args:
        df_matched_corte_sga335_sharepoint_cuismp_sga380: DataFrame containing matched records
        df_unmatched_corte_sga335_sharepoint_cuismp_sga389: DataFrame containing unmatched records
        
    Returns:
        DataFrame: A DataFrame with the failure details for Objective 1
    """
    with spark_manager.get_session_context() as spark:
        df_failures_message_matched_merged_corte_excel_sga335 = build_message_merge_sga_335_corte_excel_unmatch(df_unmatched_corte_sga335_sharepoint_cuismp_sga389)

        df_validations_cuismp_distrito_fiscal_medidas = validation_cuismp_distrito_fiscal_medidas(df_matched_corte_sga335_sharepoint_cuismp_sga380)
        df_failures_message_cuismp_distrito_fiscal_medidas = build_failure_messages_cuismp_distrito_fiscal_medidas(df_validations_cuismp_distrito_fiscal_medidas)

        df_validations_fecha_inicio_fin = validation_fecha_inicio_fin(df_matched_corte_sga335_sharepoint_cuismp_sga380)
        df_failures_message_fecha_inicio_fin = build_failure_messages_fechas_fin_inicio(df_validations_fecha_inicio_fin)

        df_validation_fin_inicio_HHMM  = validation_fin_inicio_HHMM(df_matched_corte_sga335_sharepoint_cuismp_sga380)
        df_failures_message_fecha_inicio_fin_HHMM = build_failure_messages_diff_fin_inicio_HHMM(df_validation_fin_inicio_HHMM)

        df_validation_tiempo_HHMM = validation_tiempo_HHMM_paradas_cliente(df_matched_corte_sga335_sharepoint_cuismp_sga380)
        df_failure_messages_tiempo_HHMM_paradas_cliente = buid_failure_messages_tiempo_HHMM_paradas_cliente(df_validation_tiempo_HHMM)

        df_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa = validation_tipo_caso_cid_masivo_cod_padre_determinacion(df_matched_corte_sga335_sharepoint_cuismp_sga380)
        df_failure_messages_df_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa = build_failure_messages_tipo_caso_cid_masivo_cod_padre_determinacion(df_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa)

        df_validation_averia_tipificacion_problema = validation_averia_tipificacion_problema(df_matched_corte_sga335_sharepoint_cuismp_sga380)
        df_failure_messages_validation_averia_tipificacion_problema = build_failure_messages_averia_tipificacion_problema(df_validation_averia_tipificacion_problema)

        df_validation_tipo_reporte_observacion = validation_tipo_reporte_observacion(df_matched_corte_sga335_sharepoint_cuismp_sga380)
        df_failure_message_validation_tipo_reporte_observacion = build_failure_messages_tipo_reporte_observacion(df_validation_tipo_reporte_observacion)

        df_validation_medidas_correctivas = validation_medidas_correctivas(df_matched_corte_sga335_sharepoint_cuismp_sga380)
        df_build_message_validation_medidas_correctivas = build_failure_messages_medidas_correctivas(df_validation_medidas_correctivas)
        
        df_validation_responsable = validation_responsable(df_matched_corte_sga335_sharepoint_cuismp_sga380)
        df_build_message_validation_responsable = build_failure_messages_responsable(df_validation_responsable)

        df_validation_duracion_entero = validation_duracion_entero(df_matched_corte_sga335_sharepoint_cuismp_sga380)
        df_build_message_validation_duracion_entero = build_failure_messages_duracion_entero(df_validation_duracion_entero)

        df_validation_indisponibilidad = validation_indisponibilidad(df_matched_corte_sga335_sharepoint_cuismp_sga380)
        df_build_message_validation_indisponibilidad = build_failure_messages_indisponibilidad(df_validation_indisponibilidad)

        # Concatenate all Pandas DataFrames
        dfs = [
            df_failures_message_matched_merged_corte_excel_sga335,
            df_failures_message_cuismp_distrito_fiscal_medidas,
            df_failures_message_fecha_inicio_fin,
            df_failures_message_fecha_inicio_fin_HHMM,
            df_failure_messages_tiempo_HHMM_paradas_cliente,
            df_failure_messages_df_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa,
            df_failure_messages_validation_averia_tipificacion_problema,
            df_failure_message_validation_tipo_reporte_observacion,
            df_build_message_validation_medidas_correctivas,
            df_build_message_validation_responsable,
            df_build_message_validation_duracion_entero,
            df_build_message_validation_indisponibilidad
        ]
        df_failures = pd.concat(dfs, ignore_index=True)
        return df_failures








