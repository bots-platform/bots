import pandas as pd

from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_2.o1_averias_word_validator import (
    validate_averias_word, build_failure_messages_validate_averias_word
)

from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_2.o2_informe_tecnico_validator import (
    validate_informe_tecnico_word, build_failure_messages_validate_informe_tecnico_word
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
def validation_objetivo_2(
    df_matched_word_datos_averias_corte_excel,
    df_matched_word_telefonia_averias_corte_excel,
    df_matched_word_datos_informe_tecnico_corte_excel,
    df_matched_word_telefonia_informe_tecnico_corte_excel

) -> pd.DataFrame:
    """
    Aggregates all sub-validations for Objective 2.
    Uses a common merge function and passes the merged data to each sub-validation.
    
    Returns a DataFrame with the failure details for Objective 1.
    """
    df_validate_averias_word_datos = validate_averias_word(df_matched_word_datos_averias_corte_excel, componente_word = 'COMPONENTE II')
    df_failures_message_validate_averias_word_datos = build_failure_messages_validate_averias_word(df_validate_averias_word_datos)

    df_validate_averias_word_telefonia = validate_averias_word(df_matched_word_telefonia_averias_corte_excel, componente_word = 'COMPONENTE VI')
    df_failures_message_validate_averias_word_telefono = build_failure_messages_validate_averias_word(df_validate_averias_word_telefonia)

    df_validate_informe_tecnico_word_datos = validate_informe_tecnico_word(df_matched_word_datos_informe_tecnico_corte_excel, componente_word = 'COMPONENTE II')
    df_failures_message_validate_informe_tecnico_word_datos = build_failure_messages_validate_informe_tecnico_word(df_validate_informe_tecnico_word_datos)

    df_validate_informe_tecnico_word_telefonia = validate_informe_tecnico_word(df_matched_word_telefonia_informe_tecnico_corte_excel, componente_word = 'COMPONENTE VI')
    df_failures_message_validate_informe_tecnico_word_telefono = build_failure_messages_validate_informe_tecnico_word(df_validate_informe_tecnico_word_telefonia)

    df_failures = pd.concat(
        [
        df_failures_message_validate_averias_word_datos,
        df_failures_message_validate_averias_word_telefono,
        df_failures_message_validate_informe_tecnico_word_datos,
        df_failures_message_validate_informe_tecnico_word_telefono
        ],
        ignore_index=True)

    return df_failures