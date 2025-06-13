from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import pandas as pd

from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_2.o1_averias_word_validator import (
    validate_averias_word, build_failure_messages_validate_averias_word
)

from app.modules.sga.minpub.report_validator.service.objetivos.validators.objetivo_2.o2_informe_tecnico_validator import (
    validate_informe_tecnico_word, build_failure_messages_validate_informe_tecnico_word
)

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

@log_exceptions
def run_objetivo_2(
    df_matched_word_datos_averias_corte_excel: DataFrame,
    df_matched_word_telefonia_averias_corte_excel: DataFrame,
    df_matched_word_datos_informe_tecnico_corte_excel: DataFrame,
    df_matched_word_telefonia_informe_tecnico_corte_excel: DataFrame
) -> 'pd.DataFrame':
    """
    Aggregates all sub-validations for Objective 2.
    Uses a common merge function and passes the merged data to each sub-validation.
    
    Returns a Pandas DataFrame with the failure details for Objective 2.
    All Spark operations are performed inside the context manager.
    """
    with spark_manager.get_session_context():
        # Validate averias word for datos
        df_validate_averias_word_datos = validate_averias_word(
            df_matched_word_datos_averias_corte_excel, 
            componente_word='COMPONENTE II'
        )
        df_failures_message_validate_averias_word_datos = build_failure_messages_validate_averias_word(
            df_validate_averias_word_datos
        )

        # Validate averias word for telefonia
        df_validate_averias_word_telefonia = validate_averias_word(
            df_matched_word_telefonia_averias_corte_excel, 
            componente_word='COMPONENTE IV'
        )
        df_failures_message_validate_averias_word_telefono = build_failure_messages_validate_averias_word(
            df_validate_averias_word_telefonia
        )

        # Validate informe tecnico word for datos
        df_validate_informe_tecnico_word_datos = validate_informe_tecnico_word(
            df_matched_word_datos_informe_tecnico_corte_excel, 
            componente_word='COMPONENTE II'
        )
        df_failures_message_validate_informe_tecnico_word_datos = build_failure_messages_validate_informe_tecnico_word(
            df_validate_informe_tecnico_word_datos
        )

        # Validate informe tecnico word for telefonia
        df_validate_informe_tecnico_word_telefonia = validate_informe_tecnico_word(
            df_matched_word_telefonia_informe_tecnico_corte_excel, 
            componente_word='COMPONENTE IV'
        )
        df_failures_message_validate_informe_tecnico_word_telefono = build_failure_messages_validate_informe_tecnico_word(
            df_validate_informe_tecnico_word_telefonia
        )

        # Concatenate all failure DataFrames using pandas
        df_failures = pd.concat([
            df_failures_message_validate_averias_word_datos,
            df_failures_message_validate_averias_word_telefono,
            df_failures_message_validate_informe_tecnico_word_datos,
            df_failures_message_validate_informe_tecnico_word_telefono
        ], ignore_index=True)

        return df_failures