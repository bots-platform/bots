
import pandas as pd
import numpy as np
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

    df['fecha_generacion_truncated']    = df['fecha_generacion'].dt.floor('T')
    df['interrupcion_inicio_truncated'] = df['interrupcion_inicio'].dt.floor('T')
    df['interrupcion_fin_truncated']    = df['interrupcion_fin'].dt.floor('T')

    mask_masivo = df['masivo'] == "Si"
    df['Expected_Inicio_truncated'] = np.where(
        mask_masivo,
        df['fecha_generacion_truncated'],
        df['interrupcion_inicio_truncated']
    )

    neg_mask = mask_masivo & (
        df['interrupcion_fin_truncated'] - df['Expected_Inicio_truncated'] < pd.Timedelta(0)
    )
    df.loc[neg_mask, 'Expected_Inicio_truncated'] = df.loc[neg_mask, 'interrupcion_inicio_truncated']

    df['Expected_Inicio_truncated_fm'] = (
        df['Expected_Inicio_truncated']
        .dt.strftime('%d/%m/%Y %H:%M')
        .fillna("N/A")
        .astype(str)
    )

    df['interrupcion_fin_truncated_fm'] = (
        df['interrupcion_fin_truncated']
        .dt.strftime('%d/%m/%Y %H:%M')
        .fillna("N/A")
        .astype(str)
    )


    return df
