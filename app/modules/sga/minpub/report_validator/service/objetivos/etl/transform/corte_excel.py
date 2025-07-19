import pandas as pd
import numpy as np
from app.modules.sga.minpub.report_validator.service.objetivos.utils.cleaning import (
    handle_null_values, cut_decimal_part
)
from app.modules.sga.minpub.report_validator.service.objetivos.utils.etl_utils import (
    to_hhmm,
    parse_hhmm_to_minutes,
    hhmm_to_minutes,
    extract_total_hours
)
from app.modules.sga.minpub.report_validator.service.objetivos.utils.calculations import count_A_traves_mayus

def preprocess_corte_excel(df: pd.DataFrame) -> pd.DataFrame:
    """
    …same docstring…
    """
    df = df.copy()
    df = cut_decimal_part(df, 'CUISMP')
    df = handle_null_values(df)
    df = df.rename(columns={'TICKET':'nro_incidencia'})

    cols_to_str = [
        'nro_incidencia',
        'CODINCIDENCEPADRE',
        'CUISMP',
        'DF',
        'DETERMINACIÓN DE LA CAUSA',
        'TIPO CASO',
        'CID',
        'MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'
    ]
    df = df.assign(**{
        col: df[col].astype(str).str.strip().replace({'': 'No disponible'})
        for col in cols_to_str
    })

    df['num_A_traves'] = df['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'].apply(count_A_traves_mayus)

    df[['FECHA Y HORA INICIO','FECHA Y HORA FIN']] = df[
        ['FECHA Y HORA INICIO','FECHA Y HORA FIN']
    ].apply(pd.to_datetime, format='%Y-%m-%d', errors='coerce')

 
    df['TIEMPO (HH:MM)_trimed'] = np.where(
        df['TIEMPO (HH:MM)'].str.endswith(':00', na=False),
        df['TIEMPO (HH:MM)'].str[:5],
        df['TIEMPO (HH:MM)']
    )

    # 4b) FIN-INICIO trim
    df['FIN-INICIO (HH:MM)_trimed'] = df['FIN-INICIO (HH:MM)'].apply(to_hhmm)

    # 5) formats & durations in two chained assigns
    df = (
        df
        .assign(
            FECHA_Y_HORA_INICIO_fmt=lambda d: d['FECHA Y HORA INICIO']
                .dt.strftime('%d/%m/%Y %H:%M').fillna("N/A"),
            FECHA_Y_HORA_FIN_fmt=lambda d: d['FECHA Y HORA FIN']
                .dt.strftime('%d/%m/%Y %H:%M').fillna("N/A")
        )
        .assign(
            duration_diff_corte_sec=lambda d: (
                d['FECHA Y HORA FIN'] - d['FECHA Y HORA INICIO']
            ),
            diff_corte_sec_hhmm=lambda d: (
                d['duration_diff_corte_sec']
                 .dt.total_seconds().floordiv(3600).fillna(0).astype(int).astype(str).str.zfill(2)
                + ":"
                + d['duration_diff_corte_sec']
                 .dt.total_seconds().mod(3600).floordiv(60).fillna(0).astype(int).astype(str).str.zfill(2)
            ),
            fin_inicio_hhmm_column_corte_to_minutes=lambda d: pd.to_numeric(
                d['FIN-INICIO (HH:MM)_trimed'].apply(parse_hhmm_to_minutes),
                errors='coerce'
            ),
            duration_diff_corte_min=lambda d: d['diff_corte_sec_hhmm']
                .apply(lambda hhmm: hhmm_to_minutes(hhmm)),
            extracted_hour=lambda d: d['TIEMPO (HH:MM)']
                .apply(extract_total_hours).fillna(0).astype(int)
        )
    )

    return df

def safe_nunique(series):
    """
    Devuelve nunique seguro: si la serie contiene listas, las convierte a string antes de calcular nunique.
    """
    if series.apply(lambda x: isinstance(x, list)).any():
        return series.apply(str).nunique()
    else:
        return series.nunique()


def get_dataframe_summary(df):
    """
    Devuelve un resumen del DataFrame, incluyendo tipo de dato, conteo de no nulos, nulos, porcentaje de nulos y valores únicos.
    No falla si hay columnas con listas.
    """
    import pandas as pd
    summary_df = pd.DataFrame({
        'Data Type': df.dtypes,
        'Non Null Count': df.count(),
        'Null Count': df.isna().sum(),
        'Null Percentage': (df.isna().sum() / len(df) * 100).round(2),
        'Unique Values': [safe_nunique(df[col]) for col in df.columns],
    })
    return summary_df
