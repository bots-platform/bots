

import pandas as pd

def preprocess_df_word_averias(df):
    
    df = df.rename(columns={'Número de ticket':'nro_incidencia'})
    df['nro_incidencia'] = df['nro_incidencia'].astype(str)
    df = df.rename(columns={'Tiempo\nTotal (HH:MM)': 'Tiempo Total (HH:MM)'})
    df['Fecha y Hora Inicio'] = pd.to_datetime(df['Fecha y Hora Inicio'], format='%d/%m/%Y %H:%M', errors='coerce')
    df['Fecha y Hora Fin'] = pd.to_datetime(df['Fecha y Hora Fin'], format='%d/%m/%Y %H:%M', errors='coerce')
    return df

