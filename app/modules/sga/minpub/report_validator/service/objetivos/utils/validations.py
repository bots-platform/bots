import pandas as pd


def validate_required_columns_from_excel(path_corte_excel, required_columns, skipfooter=0):
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
            path_corte_excel,
            dtype=str,
            skipfooter=skipfooter,
            engine="openpyxl"
        )
    except Exception as e:
        raise ValueError(f"Error 400: No se pudo leer el Excel: {e}")


    df.columns = df.columns.str.strip()


    faltantes = [c for c in required_columns if c not in df.columns]
    if faltantes:
        raise ValueError(f"Error 400: Faltan columnas: {faltantes}")


    df_clean = (
        df[required_columns]
        .apply(lambda col: col.str.replace("_x000D_", "", regex=False).str.strip())
        .fillna("-")
    )
    return df_clean
