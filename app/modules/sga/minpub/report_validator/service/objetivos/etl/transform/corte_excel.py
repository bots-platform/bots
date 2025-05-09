
import re
import pandas as pd
import numpy as np

from app.modules.sga.minpub.report_validator.service.objetivos.utils.cleaning import ( 
    handle_null_values, cut_decimal_part
)

def preprocess_corte_excel(df):

    df = df.copy()

    df = cut_decimal_part(df,'CUISMP')
    df["CODINCIDENCEPADRE"] = df["CODINCIDENCEPADRE"].astype(str).str.strip().fillna('No disponible')
    df = handle_null_values(df)
    df = df.rename(columns={'TICKET':'nro_incidencia'})
    df['nro_incidencia'] = df['nro_incidencia'].astype(str)
    df['DF'] = df['DF'].astype(str).str.strip().fillna('No disponible')
    df['CUISMP'] = df['CUISMP'].astype(str).str.strip().fillna('No disponible')
    df['DETERMINACIÓN DE LA CAUSA'] = df['DETERMINACIÓN DE LA CAUSA'].astype(str).str.strip().fillna("No disponible")
    df['TIPO CASO'] = df['TIPO CASO'].astype(str).str.strip().fillna("No disponible")
    df['CID'] = df['CID'].astype(str).str.strip().fillna("No disponible")
    df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'] = df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'].astype(str).str.strip().fillna("No disponible")
    df['FECHA Y HORA INICIO'] = pd.to_datetime(df['FECHA Y HORA INICIO'], format='%Y-%m-%d', errors='coerce')
    df['FECHA Y HORA FIN'] = pd.to_datetime(df['FECHA Y HORA FIN'], format='%Y-%m-%d', errors='coerce')
    df['TIEMPO (HH:MM)_trimed'] = df['TIEMPO (HH:MM)'].apply(
        lambda x: str(x)[:5] if isinstance(x, str) and x.endswith(":00") else x
    )

    pat = re.compile(
        r'^(?:(?P<days>\d+)\s+days?,\s*)?'   
        r'(?P<hours>\d+):(?P<minutes>\d{2})'  
        r'(?:[:]\d{2})?'                    
        r'\.?$'                            
    )

    def to_hhmm(x):
        s = str(x).strip()
        m = pat.match(s)
        if not m:
            return pd.NA 
        days = int(m.group('days')) if m.group('days') else 0
        hrs  = int(m.group('hours'))
        mins = int(m.group('minutes'))
        total_h = days * 24 + hrs
        return f"{total_h:02d}:{mins:02d}"


    df['FIN-INICIO (HH:MM)_trimed'] = df['FIN-INICIO (HH:MM)'].apply(to_hhmm)

    df['FECHA_Y_HORA_INICIO_fmt'] = (
        df['FECHA Y HORA INICIO']
        .dt.strftime('%d/%m/%Y %H:%M')
        .fillna("N/A")
        .astype(str)
    )

    df['FECHA_Y_HORA_FIN_fmt'] = (
        df['FECHA Y HORA FIN']
        .dt.strftime('%d/%m/%Y %H:%M')
        .fillna("N/A")
        .astype(str)
    )

    df['duration_diff_corte_sec'] = (df['FECHA Y HORA FIN'] - df['FECHA Y HORA INICIO'])
    df['diff_corte_sec_hhmm'] = df['duration_diff_corte_sec'].apply(lambda x: f"{int(x.total_seconds() // 3600):02}:{int(x.total_seconds() % 3600 // 60):02d}")


    def parse_hhmm_to_minutes(hhmm_str):
        if pd.isna(hhmm_str):
            return np.nan
        try:
            h,m = str(hhmm_str).split(':')
            total_minutes = float(h) * 60 + float(m)
            print(f"Converted {hhmm_str} to {total_minutes} seconds")
            return total_minutes
        except Exception as e: 
            print(f"Error with {hhmm_str}: {e}")
            return np.nan
    
    df['fin_inicio_hhmm_column_corte_to_minutes'] = df['FIN-INICIO (HH:MM)_trimed'].apply(parse_hhmm_to_minutes)

    def hhmm_to_minutes(hhmm_str):
        hh, mm = hhmm_str.split(":")
        return int(hh) * 60 + int (mm)

 
    df['duration_diff_corte_min'] = df['diff_corte_sec_hhmm'].apply(hhmm_to_minutes)


    return df
