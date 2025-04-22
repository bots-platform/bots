import pandas as pd

from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_2.word_extractor import extract_averias_table

from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_2.o1_averias_word_validator import (
    validate_averias_word
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
    df_matched_word_datos_corte_excel,
    df_matched_word_telefonia_corte_excel, 
) -> pd.DataFrame:
    """
    Aggregates all sub-validations for Objective 2.
    Uses a common merge function and passes the merged data to each sub-validation.
    
    Returns a DataFrame with the failure details for Objective 1.
    """
    df_validate_averias_word = validate_averias_word(df_matched_word_datos_corte_excel)
    df_failures_message_validate_averias_word = 

    df_failures = pd.concat(
        [
        df_failures_message_matched_merged_corte_excel_sga335,
    
        ],
        ignore_index=True)

    return df_failures