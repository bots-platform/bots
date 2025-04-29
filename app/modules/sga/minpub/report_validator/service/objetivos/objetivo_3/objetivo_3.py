import pandas as pd


from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_3.o1_anexos_sustentos_paradas_reloj_validator    import (
    validate_anexos_indisponibilidad_word, build_failure_messages_validate_anexos_indisponibilidad_word
)


from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)


@log_exceptions
def validation_objetivo_3(
 df_matched_word_datos_anexo_indisponibilidad_df_merged_sga,
 df_matched_word_telefonia_anexo_indisponibilidad_df_merged_sga
   

) -> pd.DataFrame:
    """
    Aggregates all sub-validations for Objective 2.
    Uses a common merge function and passes the merged data to each sub-validation.
    
    Returns a DataFrame with the failure details for Objective 1.
    """
    df_validate_anexos_indisponibilidad_word_datos = validate_anexos_indisponibilidad_word(df_matched_word_datos_anexo_indisponibilidad_df_merged_sga)
    df_failures_message_validate_anexos_indisponibilidad_word_datos = build_failure_messages_validate_anexos_indisponibilidad_word(df_validate_anexos_indisponibilidad_word_datos)

    df_validate_anexos_indisponibilidad_word_telefonia = validate_anexos_indisponibilidad_word(df_matched_word_telefonia_anexo_indisponibilidad_df_merged_sga)
    df_failures_message_validate_anexos_indisponibilidad_word_telefonia = build_failure_messages_validate_anexos_indisponibilidad_word(df_validate_anexos_indisponibilidad_word_telefonia)    

    df_failures = pd.concat(
        [
        df_failures_message_validate_anexos_indisponibilidad_word_datos,
        df_failures_message_validate_anexos_indisponibilidad_word_telefonia
        ],
        ignore_index=True)

    return df_failures
