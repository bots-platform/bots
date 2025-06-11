from typing import List, Dict
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, FloatType, TimestampType, DoubleType, IntegerType
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def cut_decimal_part(df, column):
    """
    Converts a DataFrame column from float (or numeric string) to a string
    by removing the decimal part (i.e. converting 13.5 to "13", 12.0 to "12").
    Non-numeric values are converted to null and then to an empty string.

    Parameters:
        df (pyspark.sql.DataFrame): The input PySpark DataFrame.
        column (str): The name of the column to process.
    
    Returns:
        pyspark.sql.DataFrame: The DataFrame with the processed column.
    """
    # First convert to double to handle numeric strings
    df = df.withColumn(column, F.col(column).cast(DoubleType()))
    
    # Convert to integer and then to string, handling nulls
    df = df.withColumn(
        column,
        F.when(F.col(column).isNotNull(), 
               F.cast(F.cast(F.col(column), IntegerType()), StringType()))
        .otherwise(F.lit(""))
    )
    
    return df

@log_exceptions
def handle_null_values(df, fill_str="", fill_float=0.0, fill_datetime=""):
    """
    Fill null values in PySpark DataFrame columns based on data type.

    Parameters:
        df (pyspark.sql.DataFrame): The input PySpark DataFrame.
        fill_str (str): Value to replace nulls in string columns. Default is "".
        fill_float (float): Value to replace nulls in float columns. Default is 0.0.
        fill_datetime: Value to replace nulls in timestamp columns. 
                      Default is "", but you can also pass a default datetime.
    
    Returns:
        pyspark.sql.DataFrame: The DataFrame with nulls handled.
    """
    # Get column types
    schema = df.schema
    
    # Handle string columns
    for field in schema:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.coalesce(F.col(field.name), F.lit(fill_str)))
    
    # Handle float columns
    for field in schema:
        if isinstance(field.dataType, (FloatType, DoubleType)):
            df = df.withColumn(field.name, F.coalesce(F.col(field.name), F.lit(fill_float)))
    
    # Handle timestamp columns
    for field in schema:
        if isinstance(field.dataType, TimestampType):
            df = df.withColumn(field.name, F.coalesce(F.col(field.name), F.lit(fill_datetime)))
    
    return df

@log_exceptions
def handle_null_values_spark(df, fill_str="", fill_float=0.0, fill_datetime=""):
    """
    Fill null values in PySpark DataFrame columns based on data type.

    Parameters:
        df (pyspark.sql.DataFrame): The input PySpark DataFrame.
        fill_str (str): Value to replace nulls in string columns. Default is "".
        fill_float (float): Value to replace nulls in float columns. Default is 0.0.
        fill_datetime: Value to replace nulls in timestamp columns. 
                      Default is "", but you can also pass a default datetime.
    
    Returns:
        pyspark.sql.DataFrame: The DataFrame with nulls handled.
    """
    # Get column types
    schema = df.schema
    
    # Handle string columns
    for field in schema:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, F.coalesce(F.col(field.name), F.lit(fill_str)))
    
    # Handle float columns
    for field in schema:
        if isinstance(field.dataType, FloatType):
            df = df.withColumn(field.name, F.coalesce(F.col(field.name), F.lit(fill_float)))
    
    # Handle timestamp columns
    for field in schema:
        if isinstance(field.dataType, TimestampType):
            df = df.withColumn(field.name, F.coalesce(F.col(field.name), F.lit(fill_datetime)))
    
    return df