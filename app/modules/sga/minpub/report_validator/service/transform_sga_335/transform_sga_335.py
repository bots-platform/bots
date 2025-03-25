import pandas as pd
from pathlib import Path
pd.set_option('display.max_rows', None)
import re

BASE_DIR = Path.cwd().parent.parent.parent.parent.parent.parent.parent
SAVE_DIR_EXTRACT_SGA_335 = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "sga_335"/"minpuSGA.xlsx"  

df = pd.read_excel(SAVE_DIR_EXTRACT_SGA_335)
df.head(3)

def determine_tipo_reporte(row):
    
    if row['nro_incidencia'] in ["21713978", "21706774"]:
        return "PROACTIVO"
    if row['tipo_caso'] == "OTROS CALIDAD-MONITOREO" and row['tipo_servicio'] == "Acceso Dedicado a Internet":
        return "PROACTIVO"
    if row['tipo_caso'] == "ENLACE INTERMITENTE - MONITOREO" and row['tipo_servicio'] == "Red Privada Virtual Local":
        return "PROACTIVO"
    #if row['Conteo para proactivo'] == 1:
        return "PROACTIVO"
    return "RECLAMO"

df.fillna({"nro_incidencia": "", "tipo_caso": "", "tipo_servicio": "", "Conteo para proactivo": 0}, inplace= True)
df["tipo_reporte"] = df.apply(determine_tipo_reporte, axis=1)


def determine_component(row):

    if row["tipo_reporte"] == "PROACTIVO":
        return "COMPONENTE II"

    causa = str(row["it_determinacion_de_la_causa"]) if pd.notna(row["it_determinacion_de_la_causa"]) else ""
    
    roman_numerals = {1: "I", 2: "II", 3: "III", 4: "IV", 5: "V"}

    for i in range(5, 0, -1):
        if f"COMPONENTE {roman_numerals[i]}" in causa:
            return f"COMPONENTE {roman_numerals[i]}"
    if "SOLICITUD - Cliente" in row["tipo_incidencia"]:
        return "SOLICITUD"
    return "SIN COMPONENTE/MAL ESCRITO"

df["componente"] = df.apply(determine_component, axis=1)


def determine_cuismp(df):

    BASE_DIR = Path.cwd().parent.parent.parent.parent.parent.parent.parent
    file_path_cuismp_cid = BASE_DIR /  "media" / "minpub" / "validator_report" / "extract" / "sharepoint_cid_cuismp"/ "MINPU - CID-CUISMP - AB.xlsx"  
    file_path_disponibilidad_servicio_c2 = BASE_DIR /  "media" / "minpub" / "validator_report" / "extract" / "disponibilidad_servicio_c2"/ "DISPONIBILIDAD DEL SERVICIO C2.xlsx"  

    if not file_path_cuismp_cid.exists():
        raise FileNotFoundError(f" File not found {file_path_cuismp_cid}")

    if not file_path_disponibilidad_servicio_c2.exists():
        raise FileNotFoundError(f"File not found {file_path_disponibilidad_servicio_c2}")

    df_cuismp_by_cid = pd.read_excel(file_path_cuismp_cid)

    df_cuismp_by_cid = df_cuismp_by_cid.rename(columns={"CID":"cid"})

    df["cid"] = df["cid"].astype(str).fillna("")
    df_cuismp_by_cid["cid"] = df_cuismp_by_cid["cid"].astype(str).fillna("")

    df = df.merge(df_cuismp_by_cid, on="cid", how="left")
    return df
