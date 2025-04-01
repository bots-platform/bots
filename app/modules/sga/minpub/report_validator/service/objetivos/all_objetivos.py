from app.modules.sga.minpub.report_validator.service.objetivos.objetivo1 import cut_decimal_part
from app.modules.sga.minpub.report_validator.service.objetivos.objetivo1 import handle_null_values
from .objetivo1 import validation_objetivo_1
from typing import List, Dict
import pandas as pd


def all_objetivos(
    path_corte_excel, 
    path_sgq_dinamico_335, 
    path_sga_dinamico_380,
    path_cid_cuismp_sharepoint,

) -> List[Dict]:
    """
    Calls each objective's validation function and combines the results.
    Each objective function returns a DataFrame with the columns:
      - 'numero de incidencia'
      - 'mensaje'
      - 'objetivo'
    
    Returns:
      A list of dictionaries (one per incident that fails at least one validation)
      across all objectives.
    """
    results = []
    
    df_corte_excel = pd.read_excel(path_corte_excel, skipfooter=2, engine="openpyxl")
    df_sga_dinamico_335 = pd.read_excel(path_sgq_dinamico_335) 
    df_sga_dinamico_380 = pd.read_excel(path_sga_dinamico_380)
    df_cid_cuismp_sharepoint = pd.read_excel(path_cid_cuismp_sharepoint)

    df_corte_excel = cut_decimal_part(df_corte_excel,'CUISMP')
    df_corte_excel = handle_null_values(df_corte_excel)

    df_sga_dinamico_335 = handle_null_values(df_sga_dinamico_335)

    df_cid_cuismp_sharepoint = cut_decimal_part(df_cid_cuismp_sharepoint, 'CUISMP')

    obj1_df = validation_objetivo_1(df_corte_excel, df_sga_dinamico_335, df_cid_cuismp_sharepoint)
    results.extend(obj1_df.to_dict(orient='records'))
    

    return results