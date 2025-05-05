

from app.modules.sga.minpub.report_validator.service.objetivos.utils.cleaning import ( 
    cut_decimal_part
)

def preprocess_df_cid_cuismp_sharepoint(df):

    df = cut_decimal_part(df, 'CUISMP')
    df = df.rename(columns={"CID":"cid"})
    df["cid"] = df["cid"].astype(str).fillna("")
    df["Distrito Fiscal"] = df["Distrito Fiscal"].astype(str).str.strip().fillna('No disponible')
    df["CUISMP"] = df["CUISMP"].astype(str).str.strip().fillna('No disponible')
    df["SEDE"] = df["SEDE"].astype(str).str.strip().fillna('No disponible')
    
    return df

