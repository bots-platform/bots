
import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime
import language_tool_python
import re


def validate_duracion_entero(df_merged: pd.Dataframe)-> pd.DataFrame:
    """
    1) Extract  the hour to the left of ':' from 'TIEMPO (HH:MM)' -> extracted_hour
    2) Check the extracted_hour == DURACION ENTERO -> duracion_entero_ok
    3) Based on DURACION ENTERO , compute agrupacion_expected:

    0 -> 'Menor a 1h'
    1-3 -> 'Entre 1h a 4h'
    4-7 -> 'Entre 4h a 8h'
    8-23 -> 'Entre 8h a 24h'

    4) Check that Agrupacion entero == agrupacion_expected -> agrupacion_entero_ok
    5) Combine into Validation_OK & fail_count
    """
    df = df_merged.copy()

    df['extracted_hour'] = (
        df['TIEMPO (HHMM)']
        .astype(str)
        .str.split(':', n=1)
        .str[0]
        .astype(int)
    )

    df['duracion_entero_ok'] = (
        df['extracted_hour'] == df['Duracion entero']
    )

    conditions = [
        df['duracion entero'] == 0,
        df['duracion entero'].isin([1, 2,3 ]),
        df['duracion entero'].isin([4, 5, 6, 7]),
        df['duracion entero'].between(8, 23),
    ]

    choises = [
        'Menor a 1h',
        'Entre 1h a 4h',
        'Entre 4h a 8h',
        'Entre 8h a 24h',
    ]

    df['agrupacion_expected'] == np.select(conditions, choises, default='Mayor a 24h') 

    df['agrupacion_entero_ok'] == (
        df['Agrupación entero'].astype(str).str.strip()
        == df['agrupacion_expected']
    )

    df['Validation_OK'] = df['duracion_entero_ok'] & df['agrupacion_entero_ok']
    df['fail_count'] = (~df['Validation_OK']).astype(int)

    return df


