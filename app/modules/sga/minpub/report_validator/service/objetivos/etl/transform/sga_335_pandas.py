import pandas as pd
import numpy as np
from app.modules.sga.minpub.report_validator.service.objetivos.utils.cleaning import ( 
    handle_null_values, cut_decimal_part
)


def preprocess_335(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza y prepara el DataFrame SGA-335:
      1. Convierte columnas a datetime y strings limpias.
      2. Rellena nulos y corta decimales donde toca.
      3. Trunca fechas a minutos en bloque.
      4. Calcula Expected_Inicio según 'masivo'.
      5. Genera formatos legibles y duraciones en segundos, HH:MM y minutos.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame crudo de SGA-335 con al menos las columnas:
        ['interrupcion_inicio', 'interrupcion_fin', 'fecha_comunicacion_cliente',
         'fecha_generacion', 'fg_padre', 'hora_sistema', 'cid', 'nro_incidencia',
         'it_determinacion_de_la_causa', 'tipo_caso', 'codincidencepadre', 'masivo'].

    Returns
    -------
    pandas.DataFrame
        Copia enriquecida con columnas:
        - fecha_*_truncated
        - Expected_Inicio_truncated[_fm]
        - interrupcion_fin_truncated_fm
        - duration_diff_335, duration_diff_335_sec, diff_335_sec_hhmm,
          duration_diff_335_min
    """

    df = df.copy()
    dt_cols = [
        'interrupcion_inicio', 'interrupcion_fin',
        'fecha_comunicacion_cliente', 'fecha_generacion'
    ]
    df[dt_cols] = df[dt_cols].apply(pd.to_datetime, errors='coerce', dayfirst=True)
    df = handle_null_values(df)
    df = cut_decimal_part(df, 'codincidencepadre')


    for col in ['cid', 'nro_incidencia', 'it_determinacion_de_la_causa', 'tipo_caso', 'codincidencepadre']:
        df[col] = df[col].astype(str).str.strip().replace({'': 'No disponible'})
    
    trunc = df[['fecha_generacion', 'interrupcion_inicio', 'interrupcion_fin']].apply(
        lambda s: s.dt.floor('min')
    )
    trunc.columns = [
        'fecha_generacion_truncated',
        'interrupcion_inicio_truncated',
        'interrupcion_fin_truncated'
    ]
    df = pd.concat([df, trunc], axis=1)
    
    mask = df['masivo'].eq("Si")
    df['Expected_Inicio_truncated'] = np.where(
        mask, df['fecha_generacion_truncated'], df['interrupcion_inicio_truncated']
    )
    neg = mask & (df['interrupcion_fin_truncated'] < df['Expected_Inicio_truncated'])
    df.loc[neg, 'Expected_Inicio_truncated'] = df.loc[neg, 'interrupcion_inicio_truncated']
    
    df = df.assign(
        Expected_Inicio_truncated_fm = lambda d: d['Expected_Inicio_truncated']
            .dt.strftime('%d/%m/%Y %H:%M').fillna("N/A"),
        interrupcion_fin_truncated_fm = lambda d: d['interrupcion_fin_truncated']
            .dt.strftime('%d/%m/%Y %H:%M').fillna("N/A")
    )

    df['duration_diff_335'] = (
        df['interrupcion_fin_truncated'] - df['Expected_Inicio_truncated']
    )
    df = df.assign(
        duration_diff_335_sec = lambda d: (
            d['duration_diff_335'].dt.total_seconds().fillna(0).astype(int)
        ),
        diff_335_sec_hhmm = lambda d: (
            d['duration_diff_335_sec'].floordiv(3600).astype(str).str.zfill(2)
            + ":" +
            d['duration_diff_335_sec'].mod(3600).floordiv(60).astype(str).str.zfill(2)
        ),
        duration_diff_335_min = lambda d: d['duration_diff_335_sec'].floordiv(60)
    )
    
    return df 