
import pandas as pd


def preprocess_df_word_anexos_indisponibilidad(df):
    if df is None:
        cols = ['nro_incidencia', 'indisponibilidad_header', 'indisponibilidad_periodos',
                'indisponibilidad_footer', 'indisponibilidad_total']
        return pd.DataFrame(columns=cols)

    df = df.rename(columns={'ticket': 'nro_incidencia'})
    df['nro_incidencia'] = df['nro_incidencia'].astype(str)
    return df



