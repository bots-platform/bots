from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import log_exceptions
from app.modules.sga.minpub.report_validator.service.objetivos.utils.validations import validate_required_columns_from_excel

def get_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession with optimized configurations.
    """
    return (SparkSession.builder
            .appName("SGA335Processor")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate())

def create_schema() -> StructType:
    """
    Creates the schema for the SGA 335 data.
    """
    return StructType([
        StructField("nro_incidencia", StringType(), True),  # ticket
        StructField("canal_ingreso", StringType(), True),  # tipo generacion ticket
        StructField("interrupcion_inicio", TimestampType(), True),  # fecha/hora interrupcion
        StructField("fecha_generacion", TimestampType(), True),  # fecha/hora generacion
        StructField("interrupcion_fin", TimestampType(), True),  # fecha/ hora subsanacion
        StructField("cid", StringType(), True),  # CID
        StructField("tipo_caso", StringType(), True),  # Tipo Caso
        StructField("tipificacion_problema", StringType(), True),  # averia
        StructField("it_determinacion_de_la_causa", StringType(), True),  # DETERMINACION DE LA CAUSA
        StructField("it_medidas_tomadas", StringType(), True),  # MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS
        StructField("it_conclusiones", StringType(), True),  # RECOMENDACIONES
        StructField("tiempo_interrupcion", StringType(), True),  # tiempo subsanacion efectivo
        StructField("tipificacion_interrupcion", StringType(), True),  # tiempo de indisponibilidad
        StructField("tipificacion_tipo", StringType(), True),  # ATRIBUIBLE  o RESPONSABLE
        StructField("fecha_comunicacion_cliente", TimestampType(), True),  # Fecha hora solicitud
        StructField("masivo", StringType(), True),
        StructField("codincidencepadre", StringType(), True)
    ])

@log_exceptions
def extract_sga_335(path_sga_dinamico_335: str) -> DataFrame:
    """
    Extracts data from SGA 335 Excel file using PySpark with optimized configurations.
    
    Args:
        path_sga_dinamico_335 (str): Path to the SGA 335 Excel file
        
    Returns:
        DataFrame: PySpark DataFrame containing the processed data
    """
    required_columns = [
        'nro_incidencia',  # ticket
        'canal_ingreso',  # tipo generacion ticket
        'interrupcion_inicio',  # fecha/hora interrupcion
        'fecha_generacion',  # fecha/hora generacion
        'interrupcion_fin',  # fecha/ hora subsanacion
        'cid',  # CID
        'tipo_caso',  # Tipo Caso
        'tipificacion_problema',  # averia
        'it_determinacion_de_la_causa',  # DETERMINACION DE LA CAUSA
        'it_medidas_tomadas',  # MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS
        'it_conclusiones',  # RECOMENDACIONES
        'tiempo_interrupcion',  # tiempo subsanacion efectivo
        'tipificacion_interrupcion',  # tiempo de indisponibilidad
        'tipificacion_tipo',  # ATRIBUIBLE  o RESPONSABLE
        'fecha_comunicacion_cliente',  # Fecha hora solicitud  
        'masivo',
        'codincidencepadre'
    ]

    # Create SparkSession
    spark = get_spark_session()
    
    try:
        # Read Excel file with optimized configurations
        df = (spark.read
              .format("com.crealytics.spark.excel")
              .option("header", "true")
              .option("inferSchema", "false")
              .option("maxRowsInMemory", "1000")
              .option("treatEmptyValuesAsNulls", "true")
              .schema(create_schema())
              .load(path_sga_dinamico_335))
        
        # Validate required columns
        validate_required_columns_from_excel(path_sga_dinamico_335, required_columns)
        
        # Cache the DataFrame for better performance if it will be used multiple times
        df.cache()
        
        return df
        
    except Exception as e:
        spark.stop()
        raise Exception(f"Error processing SGA 335 Excel file: {str(e)}")
    finally:
        # Clean up resources
        spark.stop()


