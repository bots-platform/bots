
import pandas as pd
from app.modules.sga.minpub.report_validator.service.objetivos.utils.cleaning import ( 
    handle_null_values
)

def preprocess_380(df):

    df['startdate'] = pd.to_datetime(df['startdate'],  errors='coerce', dayfirst=True)
    df['enddate'] = pd.to_datetime(df['enddate'],  errors='coerce', dayfirst=True)
    df = handle_null_values(df)

    return df
