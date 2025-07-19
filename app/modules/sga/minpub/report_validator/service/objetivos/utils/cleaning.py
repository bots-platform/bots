
from typing import List, Dict
import pandas as pd
from datetime import datetime, timedelta
from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)

@log_exceptions
def cut_decimal_part(df, column):
    """
    Converts a DataFrame column from float (or numeric string) to a string
    by removing the decimal part (i.e. converting 13.5 to "13", 12.0 to "12").
    Non-numeric values are converted to NaN and then to an empty string.
    """
    df[column] = pd.to_numeric(df[column], errors='coerce')

    df[column] = df[column].apply(lambda x: str(int(x)) if pd.notnull(x) else '')
    
    return df

@log_exceptions
def handle_null_values(df, fill_str="", fill_float=0.0, fill_datetime=""):
    """
    Fill null values in DataFrame columns based on data type.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        fill_str (str): Value to replace nulls in object/string columns. Default is "".
        fill_float (float): Value to replace nulls in float columns. Default is 0.0.
        fill_datetime: Value to replace nulls in datetime columns. 
                       Default is "", but you can also pass a default datetime.
    
    Returns:
        pd.DataFrame: The DataFrame with nulls handled.
    """

    obj_cols = df.select_dtypes(include=['object']).columns
    for col in obj_cols:
        df[col] = df[col].fillna(fill_str).astype(str)
    

    float_cols = df.select_dtypes(include=['float64']).columns
    for col in float_cols:
        df[col] = df[col].fillna(fill_float)
        

    datetime_cols = df.select_dtypes(include=['datetime64[ns]']) 
    for col in datetime_cols:
        df[col] = df[col].fillna(fill_datetime)
        
    return df
