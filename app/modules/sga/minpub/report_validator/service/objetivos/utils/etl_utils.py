
import re
import pandas as pd
import numpy as np

_FIN_INICIO_PAT = re.compile(
    r'^(?:(?P<days>\d+)\s+days?,\s*)?'
    r'(?P<hours>\d+):(?P<minutes>\d{2})'
    r'(?:[:]\d{2})?\.?$'
)

_FIN_DAYS_HOUR_PAT = re.compile(
    r'^(?:(?P<days>\d+)\s+day[s]?,\s*)?'  
    r'(?P<hours>\d{1,2}):'   
)

def to_hhmm(s: any) -> pd._typing.Scalar:
    """
    Parse strings like "1 day, 03:45:23" or "03:12" into "HH:MM".
    Returns pd.NA on no‐match.
    """
    text = str(s).strip()
    m = _FIN_INICIO_PAT.match(text)
    if not m:
        return pd.NA
    days = int(m.group('days')) if m.group('days') else 0
    hrs  = int(m.group('hours'))
    mins = int(m.group('minutes'))
    total_h = days * 24 + hrs
    return f"{total_h:02d}:{mins:02d}"

def parse_hhmm_to_minutes(hhmm: str) -> float:
    """
    Given "HH:MM", returns total minutes as float. pd.NA or unparsable → np.nan.
    """
    if pd.isna(hhmm):
        return np.nan
    try:
        h, m = str(hhmm).split(':')
        return float(h) * 60 + float(m)
    except Exception:
        return np.nan

def hhmm_to_minutes(hhmm: str) -> int:
    """
    Given "HH:MM", returns total minutes as int (assumes valid input).
    """
    h, m = hhmm.split(':')
    return int(h) * 60 + int(m)

def extract_total_hours(s: any) -> pd._typing.Scalar:
    """
    From strings like "2 days, 05:23:12" or "07:45", returns total hours as int.
    pd.NA or unparsable → pd.NA.
    """
    text = str(s).strip()
    m = _FIN_DAYS_HOUR_PAT.match(text)
    if not m:
        return pd.NA
    days = int(m.group('days')) if m.group('days') else 0
    hrs  = int(m.group('hours'))
    return days * 24 + hrs



def get_dataframe_summary(df):
    """
    Returns a summary DataFrame for the given DataFrame.
    
    The summary includes:
      - Data Type
      - Non Null Count
      - Null Count
      - Null Percentage
      - Unique Values count
    """
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    
    summary_df = pd.DataFrame({
        'Data Type': df.dtypes,
        'Non Null Count': df.count(),
        'Null Count': df.isna().sum(),
        'Null Percentage': (df.isna().sum() / len(df) * 100).round(2),
        'Unique Values': [df[col].nunique() for col in df.columns],
    })
    
    return summary_df