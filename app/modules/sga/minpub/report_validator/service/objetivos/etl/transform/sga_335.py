from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, DoubleType
)
from pyspark.sql.functions import (
    col, when, trim, to_timestamp, date_format,
    floor, concat, lpad, expr, unix_timestamp,
    from_unixtime, lit
)
from app.modules.sga.minpub.report_validator.service.objetivos.utils.cleaning import (
    handle_null_values, cut_decimal_part
)

def get_spark_session() -> SparkSession:
    """
    Creates and returns a SparkSession with optimized configurations.
    """
    return (SparkSession.builder
            .appName("SGA335Processor")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate())

def create_empty_schema() -> StructType:
    """
    Creates the schema for an empty SGA-335 DataFrame.
    """
    return StructType([
        StructField("interrupcion_inicio", TimestampType(), True),
        StructField("interrupcion_fin", TimestampType(), True),
        StructField("fecha_comunicacion_cliente", TimestampType(), True),
        StructField("fecha_generacion", TimestampType(), True),
        StructField("fg_padre", StringType(), True),
        StructField("hora_sistema", StringType(), True),
        StructField("cid", StringType(), True),
        StructField("nro_incidencia", StringType(), True),
        StructField("it_determinacion_de_la_causa", StringType(), True),
        StructField("tipo_caso", StringType(), True),
        StructField("codincidencepadre", StringType(), True),
        StructField("masivo", StringType(), True)
    ])

def preprocess_335(df: Optional[DataFrame] = None) -> DataFrame:
    """
    Normaliza y prepara el DataFrame SGA-335 usando PySpark:
      1. Convierte columnas a datetime y strings limpias.
      2. Rellena nulos y corta decimales donde toca.
      3. Trunca fechas a minutos en bloque.
      4. Calcula Expected_Inicio según 'masivo'.
      5. Genera formatos legibles y duraciones en segundos, HH:MM y minutos.

    Parameters
    ----------
    df : Optional[DataFrame]
        DataFrame crudo de SGA-335 con al menos las columnas:
        ['interrupcion_inicio', 'interrupcion_fin', 'fecha_comunicacion_cliente',
         'fecha_generacion', 'fg_padre', 'hora_sistema', 'cid', 'nro_incidencia',
         'it_determinacion_de_la_causa', 'tipo_caso', 'codincidencepadre', 'masivo'].

    Returns
    -------
    DataFrame
        DataFrame enriquecido con columnas:
        - fecha_*_truncated
        - Expected_Inicio_truncated[_fm]
        - interrupcion_fin_truncated_fm
        - duration_diff_335, duration_diff_335_sec, diff_335_sec_hhmm,
          duration_diff_335_min
    """
    spark = get_spark_session()
    
    try:
        if df is None:
            return spark.createDataFrame([], schema=create_empty_schema())
        
        # Convert datetime columns
        dt_cols = [
            'interrupcion_inicio', 'interrupcion_fin',
            'fecha_comunicacion_cliente', 'fecha_generacion'
        ]
        
        for col_name in dt_cols:
            df = df.withColumn(
                col_name,
                to_timestamp(col(col_name), "dd/MM/yyyy HH:mm:ss")
            )
        
        # Handle null values and clean string columns
        df = handle_null_values(df)
        df = cut_decimal_part(df, 'codincidencepadre')
        
        # Clean string columns
        string_cols = ['cid', 'nro_incidencia', 'it_determinacion_de_la_causa', 
                      'tipo_caso', 'codincidencepadre']
        
        for col_name in string_cols:
            df = df.withColumn(
                col_name,
                when(trim(col(col_name)) == "", "No disponible")
                .otherwise(trim(col(col_name)))
            )
        
        # Truncate timestamps to minutes
        df = df.withColumn(
            "fecha_generacion_truncated",
            expr("date_trunc('minute', fecha_generacion)")
        ).withColumn(
            "interrupcion_inicio_truncated",
            expr("date_trunc('minute', interrupcion_inicio)")
        ).withColumn(
            "interrupcion_fin_truncated",
            expr("date_trunc('minute', interrupcion_fin)")
        )
        
        # Calculate Expected_Inicio_truncated
        df = df.withColumn(
            "Expected_Inicio_truncated",
            when(col("masivo") == "Si", col("fecha_generacion_truncated"))
            .otherwise(col("interrupcion_inicio_truncated"))
        )
        
        # Handle negative durations
        df = df.withColumn(
            "Expected_Inicio_truncated",
            when(
                (col("masivo") == "Si") & 
                (col("interrupcion_fin_truncated") < col("Expected_Inicio_truncated")),
                col("interrupcion_inicio_truncated")
            ).otherwise(col("Expected_Inicio_truncated"))
        )
        
        # Format dates
        df = df.withColumn(
            "Expected_Inicio_truncated_fm",
            date_format(col("Expected_Inicio_truncated"), "dd/MM/yyyy HH:mm")
        ).withColumn(
            "interrupcion_fin_truncated_fm",
            date_format(col("interrupcion_fin_truncated"), "dd/MM/yyyy HH:mm")
        )
        
        # Calculate durations
        df = df.withColumn(
            "duration_diff_335_sec",
            floor(
                (unix_timestamp(col("interrupcion_fin_truncated")) - 
                 unix_timestamp(col("Expected_Inicio_truncated")))
            )
        )
        
        # Format duration as HH:MM
        df = df.withColumn(
            "diff_335_sec_hhmm",
            concat(
                lpad(floor(col("duration_diff_335_sec") / 3600).cast("string"), 2, "0"),
                lit(":"),
                lpad(floor((col("duration_diff_335_sec") % 3600) / 60).cast("string"), 2, "0")
            )
        )
        
        # Calculate duration in minutes
        df = df.withColumn(
            "duration_diff_335_min",
            floor(col("duration_diff_335_sec") / 60)
        )
        
        # Cache the DataFrame for better performance
        df.cache()
        
        return df
        
    except Exception as e:
        spark.stop()
        raise Exception(f"Error preprocessing SGA-335 DataFrame: {str(e)}")
    finally:
        spark.stop()





