from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Dict
from datetime import datetime

from app.core.spark_manager import spark_manager
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def build_message_merge_sga_335_corte_excel_unmatch(df: DataFrame) -> DataFrame:
    """
    Builds a message for unmatched records between SGA-335 and Excel report.
    
    Args:
        df: DataFrame containing unmatched records with columns:
            - nro_incidencia
            - FECHA_Y_HORA_INICIO_fmt
            - FECHA_Y_HORA_FIN_fmt
            - TIPO REPORTE
            
    Returns:
        DataFrame with columns:
            - nro_incidencia
            - mensaje (error message)
            - TIPO REPORTE
            - objetivo (set to "1.0")
    """
    with spark_manager.get_session():
        df = df.withColumn(
            "mensaje",
            F.concat(
                F.lit("Nro Incidencia "),
                F.col("nro_incidencia"),
                F.lit(" con FECHA Y HORA INICIO DE CORTE EXCEL "),
                F.col("FECHA_Y_HORA_INICIO_fmt"),
                F.lit(" y FECHA HORA FIN DE CORTE EXCEL  "),
                F.col("FECHA_Y_HORA_FIN_fmt"),
                F.lit(" no se encuentra en el reporte dinamico SGA - 335 para el rango de fecha de este corte \n")
            )
        )
        
        df = df.withColumn("objetivo", F.lit("1.0"))
        
        return df.select("nro_incidencia", "mensaje", "TIPO REPORTE", "objetivo")

