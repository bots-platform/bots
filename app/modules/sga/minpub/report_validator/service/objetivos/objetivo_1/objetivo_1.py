import pandas as pd


from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_1.unmatched_messages import (
    build_message_merge_sga_335_corte_excel_unmatch
)

from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_1.o1_cuismp_distrito_fiscal_medidas import (
    validation_cuismp_distrito_fiscal_medidas,
    build_failure_messages_cuismp_distrito_fiscal_medidas
)

from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_1.o2_fecha_inicio_fin import (
    validation_fecha_inicio_fin,
    build_failure_messages_fechas_fin_inicio
)

from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_1.o3_fin_inicio_HHMM import (
    validation_fin_inicio_HHMM,
    build_failure_messages_diff_fin_inicio_HHMM
)

from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_1.o4_tiempo_HHMM_paradas_cliente import (
    validation_tiempo_HHMM_paradas_cliente,
    buid_failure_messages_tiempo_HHMM_paradas_cliente
)

from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_1.o5_tipo_caso_cid_masivo_cod_padre_determinacion import (
    validation_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa,
    build_failure_messages_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa
)


from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_1.o6_averia_tipificacion_problema import (
    validation_averia_tipificacion_problema,
    build_failure_messages_averia
)

from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_1.o7_tipo_reporte_observacion import (
    validation_tipo_reporte_observacion,
    build_failure_messages_reporte_observacion
)


from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_1.o8_medidas_correctivas import (
    validation_medidas_correctivas,
    build_failure_messages_medidas_correctivas
)


from utils.logger_config import get_sga_logger
 
logger = get_sga_logger()

def log_exceptions(func):
    """
    Decorator to log exceptions in a function using the shared 'logger'.
    It will also re-raise the exception so that the caller can handle it
    appropriately (e.g., fail fast or continue).
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            logger.error(
                f"Error in function '{func.__name__}': {exc}",
                exc_info=True
            )
            # Optionally, decide whether to re-raise or swallow the exception.
            # Usually best practice is to re-raise so the pipeline can decide what to do:
            raise
    return wrapper


@log_exceptions
def validation_objetivo_1(
    df_matched_corte_sga335_sharepoint_cuismp_sga380,
    df_unmatched_corte_sga335_sharepoint_cuismp_sga389
) -> pd.DataFrame:
    """
    Aggregates all sub-validations for Objective 1.
    Uses a common merge function and passes the merged data to each sub-validation.
    
    Returns a DataFrame with the failure details for Objective 1.
    """

    df_failures_message_matched_merged_corte_excel_sga335 = build_message_merge_sga_335_corte_excel_unmatch(df_unmatched_corte_sga335_sharepoint_cuismp_sga389)


    df_validations_cuismp_distrito_fiscal_medidas = validation_cuismp_distrito_fiscal_medidas(df_matched_corte_sga335_sharepoint_cuismp_sga380)
    df_failures_message_cuismp_distrito_fiscal_medidas = build_failure_messages_cuismp_distrito_fiscal_medidas(df_validations_cuismp_distrito_fiscal_medidas)



    df_validations_fecha_inicio_fin = validation_fecha_inicio_fin(df_matched_corte_sga335_sharepoint_cuismp_sga380)
    df_failures_message_fecha_inicio_fin = build_failure_messages_fechas_fin_inicio(df_validations_fecha_inicio_fin)


    df_validation_fin_inicio_HHMM  = validation_fin_inicio_HHMM(df_matched_corte_sga335_sharepoint_cuismp_sga380)
    df_failures_message_fecha_inicio_fin_HHMM = build_failure_messages_diff_fin_inicio_HHMM(df_validation_fin_inicio_HHMM)


   
    df_validation_tiempo_HHMM = validation_tiempo_HHMM_paradas_cliente(df_matched_corte_sga335_sharepoint_cuismp_sga380)
    df_failure_messages_tiempo_HHMM_paradas_cliente = buid_failure_messages_tiempo_HHMM_paradas_cliente(df_validation_tiempo_HHMM)


    df_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa = validation_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa(df_matched_corte_sga335_sharepoint_cuismp_sga380)
    df_failure_messages_df_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa = build_failure_messages_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa(df_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa)

    
    df_validation_averia_tipificacion_problema = validation_averia_tipificacion_problema(df_matched_corte_sga335_sharepoint_cuismp_sga380)
    df_failure_messages_validation_averia_tipificacion_problema = build_failure_messages_averia(df_validation_averia_tipificacion_problema)

    df_validation_tipo_reporte_observacion = validation_tipo_reporte_observacion(df_matched_corte_sga335_sharepoint_cuismp_sga380)
    df_failure_message_validation_tipo_reporte_observacion = build_failure_messages_reporte_observacion(df_validation_tipo_reporte_observacion)

    df_validation_medidas_correctivas = validation_medidas_correctivas(df_matched_corte_sga335_sharepoint_cuismp_sga380)
    df_build_message_validation_medidas_correctivas = build_failure_messages_medidas_correctivas(df_validation_medidas_correctivas)
 

    
    df_failures = pd.concat(
        [
        df_failures_message_matched_merged_corte_excel_sga335,
        df_failures_message_cuismp_distrito_fiscal_medidas,
        df_failures_message_fecha_inicio_fin,
        df_failures_message_fecha_inicio_fin_HHMM,
        df_failure_messages_tiempo_HHMM_paradas_cliente,
        df_failure_messages_df_tipo_caso_cid_masivo_codincidencia_padre_determinacion_causa,
        df_failure_messages_validation_averia_tipificacion_problema,
        df_failure_message_validation_tipo_reporte_observacion,
        df_build_message_validation_medidas_correctivas
        ],
        ignore_index=True)
    
    return df_failures









