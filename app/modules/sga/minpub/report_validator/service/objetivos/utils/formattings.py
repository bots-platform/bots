from typing import List, Dict
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType
from pyspark.sql import DataFrame

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def _format_interval(dt_start, dt_end) -> str:
    """
    Format a time interval into a string using PySpark date formatting.
    
    Args:
        dt_start: Start datetime
        dt_end: End datetime
        
    Returns:
        Formatted string with the interval
    """
    start_str = F.date_format(F.lit(dt_start), "dd/MM/yyyy HH:mm:ss")
    end_str = F.date_format(F.lit(dt_end), "dd/MM/yyyy HH:mm:ss")
    return f"{start_str} hasta el día {end_str}"

@log_exceptions
def make_paragraph_paradas_cliente(stops_df: DataFrame):
    """
    Create a paragraph describing client stops using PySpark operations.
    
    Args:
        stops_df: PySpark DataFrame with 'start' and 'end' timestamp columns
        
    Returns:
        Formatted string with the paragraph
    """
    if stops_df.isEmpty():
        return ""

    # Format intervals
    formatted_intervals = stops_df.select(
        F.concat(
            F.date_format(F.col("start"), "dd/MM/yyyy HH:mm:ss"),
            F.lit(" hasta el día "),
            F.date_format(F.col("end"), "dd/MM/yyyy HH:mm:ss")
        ).alias("interval")
    )
    
    # Calculate total duration
    duration_df = stops_df.select(
        F.unix_timestamp(F.col("end")) - F.unix_timestamp(F.col("start"))
    ).agg(F.sum("(unix_timestamp(end) - unix_timestamp(start))").alias("total_seconds"))
    
    total_seconds = duration_df.first()["total_seconds"]
    hh, rem = divmod(int(total_seconds), 3600)
    mm = rem // 60
    total_str = f"{hh:02d}:{mm:02d}"

    # Collect intervals
    intervals = [row["interval"] for row in formatted_intervals.collect()]
    
    header = "Se tuvo indisponibilidad por parte del cliente para continuar los trabajos el/los día(s)".strip()
    body = "\n".join(intervals)
    footer = f"(Total de horas sin acceso a la sede: {total_str} horas)"
    return f"{header}\n{body}\n{footer}"

@log_exceptions
def make_paragraph_paradas_cliente_header(stops_df: DataFrame):
    """
    Create the header for client stops paragraph.
    
    Args:
        stops_df: PySpark DataFrame with 'start' and 'end' timestamp columns
        
    Returns:
        Formatted string with the header
    """
    if stops_df.isEmpty():
        return ""

    header = (
        "Se tuvo indisponibilidad por parte del cliente "
        "para continuar los trabajos el/los día(s)"
    )
    return header

@log_exceptions
def make_paragraph_paradas_cliente_periodos(stops_df: DataFrame):
    """
    Create the periods section for client stops paragraph.
    
    Args:
        stops_df: PySpark DataFrame with 'start' and 'end' timestamp columns
        
    Returns:
        Formatted string with the periods
    """
    if stops_df.isEmpty():
        return ""

    formatted_intervals = stops_df.select(
        F.concat(
            F.date_format(F.col("start"), "dd/MM/yyyy HH:mm:ss"),
            F.lit(" hasta el día "),
            F.date_format(F.col("end"), "dd/MM/yyyy HH:mm:ss")
        ).alias("interval")
    )
    
    intervals = [row["interval"] for row in formatted_intervals.collect()]
    return "\n".join(intervals)

@log_exceptions
def make_paragraph_paradas_cliente_footer(stops_df: DataFrame):
    """
    Create the footer for client stops paragraph.
    
    Args:
        stops_df: PySpark DataFrame with 'start' and 'end' timestamp columns
        
    Returns:
        Formatted string with the footer
    """
    if stops_df.isEmpty():
        return ""

    duration_df = stops_df.select(
        F.unix_timestamp(F.col("end")) - F.unix_timestamp(F.col("start"))
    ).agg(F.sum("(unix_timestamp(end) - unix_timestamp(start))").alias("total_seconds"))
    
    total_seconds = duration_df.first()["total_seconds"]
    hh, rem = divmod(int(total_seconds), 3600)
    mm = rem // 60
    total_str = f"{hh:02d}:{mm:02d}"

    footer = f"(Total de horas sin acceso a la sede: {total_str} horas)"
    return footer

@log_exceptions
def make_paragraph_paradas_cliente_total(stops_df: DataFrame):
    """
    Calculate the total time for client stops.
    
    Args:
        stops_df: PySpark DataFrame with 'start' and 'end' timestamp columns
        
    Returns:
        String with the total time in HH:MM format
    """
    if stops_df.isEmpty():
        return ""

    duration_df = stops_df.select(
        F.unix_timestamp(F.col("end")) - F.unix_timestamp(F.col("start"))
    ).agg(F.sum("(unix_timestamp(end) - unix_timestamp(start))").alias("total_seconds"))
    
    total_seconds = duration_df.first()["total_seconds"]
    hh, rem = divmod(int(total_seconds), 3600)
    mm = rem // 60
    return f"{hh:02d}:{mm:02d}"