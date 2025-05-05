
import pandas as pd
from app.modules.sga.minpub.report_validator.service.objetivos.utils.cleaning import ( 
    handle_null_values, cut_decimal_part
)

def preprocess_335(df):

    df['interrupcion_inicio'] = pd.to_datetime(df['interrupcion_inicio'], errors='coerce', dayfirst=True)
    df['interrupcion_fin'] = pd.to_datetime(df['interrupcion_fin'], errors='coerce', dayfirst=True)
    df['fecha_comunicacion_cliente'] = pd.to_datetime(df['fecha_comunicacion_cliente'], errors='coerce', dayfirst=True)
    df['fecha_generacion'] = pd.to_datetime(df['fecha_generacion'], errors='coerce', dayfirst=True)
    df['fg_padre'] = pd.to_datetime(df['fg_padre'], errors='coerce', dayfirst=True)
    df['hora_sistema'] = pd.to_datetime(df['hora_sistema'], errors='coerce', dayfirst=True)
    df["cid"] = df["cid"].astype(str).fillna("")
    df['nro_incidencia'] = df['nro_incidencia'].astype(str)
    df = handle_null_values(df)
    df["it_determinacion_de_la_causa"] = df["it_determinacion_de_la_causa"].astype(str).str.strip().fillna('No disponible')
    df["tipo_caso"] = df["tipo_caso"].astype(str).str.strip().fillna('No disponible')
    df["cid"] = df["cid"].astype(str).str.strip().fillna('No disponible')
    df = cut_decimal_part(df, 'codincidencepadre')
    df["codincidencepadre"] = df["codincidencepadre"].astype(str).str.strip().fillna('No disponible')

    return df
