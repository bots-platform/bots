from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import log_exceptions
from app.modules.sga.minpub.report_validator.service.objetivos.utils.validations import validate_required_columns_from_excel

def get_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession with optimized configurations.
    """
    return (SparkSession.builder
            .appName("CorteExcelProcessor")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate())

def create_schema() -> StructType:
    """
    Creates the schema for the Excel data.
    """
    return StructType([
        StructField("TICKET", StringType(), True),
        StructField("FECHA Y HORA INICIO", TimestampType(), True),
        StructField("FECHA Y HORA FIN", TimestampType(), True),
        StructField("CUISMP", StringType(), True),
        StructField("TIPO CASO", StringType(), True),
        StructField("AVERÍA", StringType(), True),
        StructField("TIEMPO (HH:MM)", StringType(), True),
        StructField("COMPONENTE", StringType(), True),
        StructField("DF", StringType(), True),
        StructField("OBSERVACIÓN", StringType(), True),
        StructField("CID", StringType(), True),
        StructField("FIN-INICIO (HH:MM)", StringType(), True),
        StructField("DETERMINACIÓN DE LA CAUSA", StringType(), True),
        StructField("RESPONSABILIDAD", StringType(), True),
        StructField("TIPO REPORTE", StringType(), True),
        StructField("Duracion entero", DoubleType(), True),
        StructField("Agrupación entero", DoubleType(), True),
        StructField("CODINCIDENCEPADRE", StringType(), True),
        StructField("MASIVO", StringType(), True),
        StructField("MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS", StringType(), True),
        StructField("TIPO DE INCIDENCIA", StringType(), True),
        StructField("TIEMPO INTERRUPCION", StringType(), True),
        StructField("INDISPONIBILIDAD", StringType(), True)
    ])

@log_exceptions
def extract_corte_excel(path_corte_excel: str, skipfooter: int) -> DataFrame:
    """
    Extracts data from Excel file using PySpark with optimized configurations.
    
    Args:
        path_corte_excel (str): Path to the Excel file
        skipfooter (int): Number of rows to skip at the end of the file
        
    Returns:
        DataFrame: PySpark DataFrame containing the processed data
    """
    required_columns = [
        'TICKET', 'FECHA Y HORA INICIO', 'FECHA Y HORA FIN', 'CUISMP',
        'TIPO CASO', 'AVERÍA', 'TIEMPO (HH:MM)', 'COMPONENTE', 'DF',
        'OBSERVACIÓN', 'CID', 'FIN-INICIO (HH:MM)',
        'DETERMINACIÓN DE LA CAUSA', 'RESPONSABILIDAD', 'TIPO REPORTE',
        'Duracion entero', 'Agrupación entero', 'CODINCIDENCEPADRE',
        'MASIVO', 'MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS',
        'TIPO DE INCIDENCIA', 'TIEMPO INTERRUPCION', 'INDISPONIBILIDAD'
    ]

    # Create SparkSession
    spark = get_spark_session()
    
    try:
        # Read Excel file with optimized configurations
        df = (spark.read
              .format("com.crealytics.spark.excel")
              .option("header", "true")
              .option("inferSchema", "false")
              .option("skipfooter", str(skipfooter))
              .option("maxRowsInMemory", "1000")
              .option("treatEmptyValuesAsNulls", "true")
              .schema(create_schema())
              .load(path_corte_excel))
        
        # Validate required columns
        validate_required_columns_from_excel(path_corte_excel, required_columns, skipfooter)
        
        # Cache the DataFrame for better performance if it will be used multiple times
        df.cache()
        
        return df
        
    except Exception as e:
        spark.stop()
        raise Exception(f"Error processing Excel file: {str(e)}")
    finally:
        # Clean up resources
        spark.stop() 