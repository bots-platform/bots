import pandas as pd
from fastapi import HTTPException

def validate_required_columns_from_excel(path_excel, required_columns, skipfooter=0):
    """
    Lee un Excel y valida que contenga las columnas requeridas, opcionalmente
    descartando las últimas `skipfooter` filas.

    Parameters
    ----------
    path_corte_excel : str
        Ruta al archivo .xlsx.
    required_columns : list of str
        Lista de nombres de columnas que deben existir.
    skipfooter : int, optional
        Número de filas al final del archivo que deben ignorarse (default 0).

    Returns
    -------
    pandas.DataFrame
        Sólo con las columnas requeridas, sin filas de footer.
    
    Raises
    ------
    ValueError
        Si no se puede leer el archivo o faltan columnas.
    """
    try:

        df = pd.read_excel(
            path_excel,
            skipfooter=skipfooter,
            engine="openpyxl"
        )
    except Exception as e:
        raise ValueError(f"Error 400: No se pudo leer el Excel: {e}")


    df.columns = df.columns.str.strip()

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Faltan columnas requeridas: {missing}"
        )

    df_clean = df[required_columns].copy()


    text_cols = df_clean.select_dtypes(include="object").columns

    df_clean[text_cols] = (
        df_clean[text_cols]
        .apply(lambda col: (
            col
            .str.replace('\r', ' ', regex=False)   
            .str.replace('_x000D_', '', regex=False)
            .str.strip()
        ))
    )
    df_clean[text_cols] = df_clean[text_cols].fillna("No disponible")

    return df_clean
