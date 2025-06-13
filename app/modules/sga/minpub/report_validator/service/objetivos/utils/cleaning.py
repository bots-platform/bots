from typing import List, Dict
from datetime import datetime, timedelta
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, TimestampType, DoubleType, IntegerType
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)
from app.core.spark_manager import spark_manager

@log_exceptions
def cut_decimal_part(df, column) -> pd.DataFrame:
    """
    Converts a DataFrame column from float (or numeric string) to a string
    by removing the decimal part (i.e. converting 13.5 to "13", 12.0 to "12").
    Non-numeric values are converted to null and then to an empty string.

    Parameters:
        df (pyspark.sql.DataFrame): The input PySpark DataFrame.
        column (str): The name of the column to process.
    
    Returns:
        pd.DataFrame: The DataFrame with the processed column as a Pandas DataFrame.
    
    Notes:
        All Spark operations are performed inside the context manager.
    """
    with spark_manager.get_session_context():
        df = df.withColumn(column, F.col(column).cast(DoubleType()))
        df = df.withColumn(
            column,
            F.when(F.col(column).isNotNull(), 
                   F.cast(F.cast(F.col(column), IntegerType()), StringType()))
            .otherwise(F.lit(""))
        )
        return df.toPandas()

@log_exceptions
def handle_null_values(df, fill_str="", fill_float=0.0, fill_datetime="") -> pd.DataFrame:
    """
    Fill null values in PySpark DataFrame columns based on data type.

    Parameters:
        df (pyspark.sql.DataFrame): The input PySpark DataFrame.
        fill_str (str): Value to replace nulls in string columns. Default is "".
        fill_float (float): Value to replace nulls in float columns. Default is 0.0.
        fill_datetime: Value to replace nulls in timestamp columns. 
                      Default is "", but you can also pass a default datetime.
    
    Returns:
        pd.DataFrame: The DataFrame with nulls handled as a Pandas DataFrame.
    
    Notes:
        All Spark operations are performed inside the context manager.
    """
    with spark_manager.get_session_context():
        schema = df.schema
        for field in schema:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, F.coalesce(F.col(field.name), F.lit(fill_str)))
        for field in schema:
            if isinstance(field.dataType, (FloatType, DoubleType)):
                df = df.withColumn(field.name, F.coalesce(F.col(field.name), F.lit(fill_float)))
        for field in schema:
            if isinstance(field.dataType, TimestampType):
                df = df.withColumn(field.name, F.coalesce(F.col(field.name), F.lit(fill_datetime)))
        return df.toPandas()

@log_exceptions
def handle_null_values_spark(df, fill_str="", fill_float=0.0, fill_datetime="") -> pd.DataFrame:
    """
    Fill null values in PySpark DataFrame columns based on data type.

    Parameters:
        df (pyspark.sql.DataFrame): The input PySpark DataFrame.
        fill_str (str): Value to replace nulls in string columns. Default is "".
        fill_float (float): Value to replace nulls in float columns. Default is 0.0.
        fill_datetime: Value to replace nulls in timestamp columns. 
                      Default is "", but you can also pass a default datetime.
    
    Returns:
        pd.DataFrame: The DataFrame with nulls handled as a Pandas DataFrame.
    
    Notes:
        All Spark operations are performed inside the context manager.
    """
    with spark_manager.get_session_context():
        schema = df.schema
        for field in schema:
            if isinstance(field.dataType, StringType):
                df = df.withColumn(field.name, F.coalesce(F.col(field.name), F.lit(fill_str)))
        for field in schema:
            if isinstance(field.dataType, FloatType):
                df = df.withColumn(field.name, F.coalesce(F.col(field.name), F.lit(fill_float)))
        for field in schema:
            if isinstance(field.dataType, TimestampType):
                df = df.withColumn(field.name, F.coalesce(F.col(field.name), F.lit(fill_datetime)))
        return df.toPandas()