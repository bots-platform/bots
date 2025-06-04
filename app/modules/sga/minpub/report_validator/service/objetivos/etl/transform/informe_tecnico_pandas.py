import pandas as pd


def preprocess_df_word_informe_tecnico(df):
  
    if df is None:
        cols = ['nro_incidencia', 'Fecha y Hora Inicio', 'Fecha y Hora Fin']
        return pd.DataFrame(columns=cols)

    df = df.rename(columns={'ticket': 'nro_incidencia'})
    df['nro_incidencia'] = df['nro_incidencia'].astype(str)

    return df 