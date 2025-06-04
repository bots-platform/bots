from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
    IntegerType
)
from pyspark.sql.functions import (
    col, when, lower, concat, lit, expr,
    isnull, array_contains, array, struct
)
from utils.logger_config import get_sga_logger
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import (
    log_exceptions
)

def get_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession with optimized configurations.
    """
    return (SparkSession.builder
            .appName("CUISMPDistritoFiscalValidator")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate())

def create_empty_schema() -> StructType:
    """
    Creates the schema for an empty CUISMP Distrito Fiscal DataFrame.
    """
    return StructType([
        StructField("CUISMP_sga_dinamico_335_excel_matched", StringType(), True),
        StructField("CUISMP_sharepoint_cid_cuismp", StringType(), True),
        StructField("DF", StringType(), True),
        StructField("Distrito Fiscal", StringType(), True),
        StructField("MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS", StringType(), True),
        StructField("nro_incidencia", StringType(), True),
        StructField("TIPO REPORTE", StringType(), True)
    ])

@log_exceptions
def validation_cuismp_distrito_fiscal_medidas(merged_df: Optional[DataFrame] = None) -> DataFrame:
    """
    Valida coincidencias de CUISMP y Distrito Fiscal, y presencia de CUISMP en medidas.

    Parámetros
    ----------
    merged_df : Optional[DataFrame]
        Debe contener las columnas:
        - CUISMP_sga_dinamico_335_excel_matched
        - CUISMP_sharepoint_cid_cuismp
        - DF
        - Distrito Fiscal
        - MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS

    Devuelve
    -------
    DataFrame
        DataFrame con estas columnas añadidas:
        - CUISMP_match
        - DF_match
        - CUISMP_in_medias_tomadas
        - Validation_OK
        - fail_count
    """
    spark = get_spark_session()
    
    try:
        if merged_df is None:
            return spark.createDataFrame([], schema=create_empty_schema())
        
        df = merged_df
        
        # Validar coincidencia de CUISMP
        df = df.withColumn(
            "CUISMP_match",
            col("CUISMP_sga_dinamico_335_excel_matched") == col("CUISMP_sharepoint_cid_cuismp")
        )
        
        # Validar coincidencia de Distrito Fiscal
        df = df.withColumn(
            "DF_match",
            lower(col("DF").cast("string")) == lower(col("Distrito Fiscal").cast("string"))
        )
        
        # Validar presencia de CUISMP en medidas
        medidas_col = "MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS"
        df = df.withColumn(
            "CUISMP_in_medias_tomadas",
            when(
                isnull(col(medidas_col)) | isnull(col("CUISMP_sga_dinamico_335_excel_matched")),
                False
            ).otherwise(
                expr(f"array_contains(split({medidas_col}, ' '), CUISMP_sga_dinamico_335_excel_matched)")
            )
        )
        
        # Calcular validación general
        df = df.withColumn(
            "Validation_OK",
            col("CUISMP_match") & col("DF_match") & col("CUISMP_in_medias_tomadas")
        )
        
        # Calcular contador de fallos
        df = df.withColumn(
            "fail_count",
            when(~col("CUISMP_match"), 1).otherwise(0) +
            when(~col("DF_match"), 1).otherwise(0) +
            when(~col("CUISMP_in_medias_tomadas"), 1).otherwise(0)
        )
        
        # Cache para mejor rendimiento
        df.cache()
        
        return df
        
    except Exception as e:
        spark.stop()
        raise Exception(f"Error en validación CUISMP Distrito Fiscal: {str(e)}")
    finally:
        spark.stop()

@log_exceptions
def build_failure_messages_cuismp_distrito_fiscal_medidas(df: DataFrame) -> DataFrame:
    """
    Genera mensajes de error y filtra filas fallidas de CUISMP y Distrito Fiscal.

    Para cada fila con `fail_count > 0`, construye la columna `mensaje`
    con la descripción de las validaciones que fallaron y añade `objetivo="1.1"`.

    Parameters
    ----------
    df : DataFrame
        Debe contener:
        - Flags de validación: Validation_OK, CUISMP_match, DF_match,
          CUISMP_in_medias_tomadas, fail_count
        - Datos de CUISMP y DF: CUISMP_sharepoint_cid_cuismp,
          CUISMP_sga_dinamico_335_excel_matched, Distrito Fiscal, DF
        - Identificadores: nro_incidencia, TIPO REPORTE

    Returns
    -------
    DataFrame
        DataFrame filtrado con filas donde `fail_count > 0` y columnas:
        ['nro_incidencia', 'mensaje', 'TIPO REPORTE', 'objetivo'].
    """
    spark = get_spark_session()
    
    try:
        # Construir mensajes de error
        df = df.withColumn(
            "mensaje",
            when(
                col("Validation_OK"),
                lit("Validation successful")
            ).otherwise(
                concat(
                    when(
                        ~col("CUISMP_match"),
                        concat(
                            lit("\n CUISMP en Sharepoint CID-CUISMP: \n"),
                            col("CUISMP_sharepoint_cid_cuismp").cast("string"),
                            lit("\n es diferente a EXCEL -CORTE (CUISMP): \n"),
                            col("CUISMP_sga_dinamico_335_excel_matched").cast("string")
                        )
                    ).otherwise(lit("")),
                    when(
                        ~col("DF_match"),
                        concat(
                            lit("\n Distrito Fiscal en Sharepoint CID-CUISMP: \n"),
                            col("Distrito Fiscal").cast("string"),
                            lit("\n es diferente a EXCEL -CORTE (DF):  \n"),
                            col("DF").cast("string")
                        )
                    ).otherwise(lit("")),
                    when(
                        ~col("CUISMP_in_medias_tomadas"),
                        concat(
                            lit("\n CUISMP: \n "),
                            col("CUISMP_sga_dinamico_335_excel_matched").cast("string"),
                            lit("\n no encontrado in MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS.")
                        )
                    ).otherwise(lit(""))
                )
            )
        )
        
        # Añadir objetivo
        df = df.withColumn("objetivo", lit("1.1"))
        
        # Filtrar fallos y seleccionar columnas
        df_failures = df.filter(col("fail_count") > 0)
        df_failures = df_failures.select(
            "nro_incidencia",
            "mensaje",
            "TIPO REPORTE",
            "objetivo"
        )
        
        return df_failures
        
    except Exception as e:
        spark.stop()
        raise Exception(f"Error generando mensajes de fallo: {str(e)}")
    finally:
        spark.stop()


