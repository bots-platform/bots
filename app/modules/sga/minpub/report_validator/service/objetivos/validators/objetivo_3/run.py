from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_3.o1_anexos_sustentos_paradas_reloj_validator import (
    validate_anexos_indisponibilidad_word, build_failure_messages_validate_anexos_indisponibilidad_word
)

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def run_objetivo_3(
    df_matched_word_datos_anexo_indisponibilidad_df_merged_sga: DataFrame,
    df_matched_word_telefonia_anexo_indisponibilidad_df_merged_sga: DataFrame
) -> DataFrame:
    """
    Aggregates all sub-validations for Objective 3.
    Uses a common merge function and passes the merged data to each sub-validation.
    
    Returns a DataFrame with the failure details for Objective 3.
    """
    # Validate anexos indisponibilidad for datos
    df_validate_anexos_indisponibilidad_word_datos = validate_anexos_indisponibilidad_word(
        df_matched_word_datos_anexo_indisponibilidad_df_merged_sga
    )
    df_failures_message_validate_anexos_indisponibilidad_word_datos = build_failure_messages_validate_anexos_indisponibilidad_word(
        df_validate_anexos_indisponibilidad_word_datos
    )

    # Validate anexos indisponibilidad for telefonia
    df_validate_anexos_indisponibilidad_word_telefonia = validate_anexos_indisponibilidad_word(
        df_matched_word_telefonia_anexo_indisponibilidad_df_merged_sga
    )
    df_failures_message_validate_anexos_indisponibilidad_word_telefonia = build_failure_messages_validate_anexos_indisponibilidad_word(
        df_validate_anexos_indisponibilidad_word_telefonia
    )

    # Union all failure DataFrames
    df_failures = df_failures_message_validate_anexos_indisponibilidad_word_datos.unionAll(
        df_failures_message_validate_anexos_indisponibilidad_word_telefonia
    )

    return df_failures