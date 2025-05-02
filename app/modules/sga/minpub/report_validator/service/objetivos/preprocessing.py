
import re
from typing import List, Dict
import pandas as pd
from datetime import datetime, timedelta
from app.modules.sga.minpub.report_validator.service.objetivos.cleaning import ( 
    handle_null_values, cut_decimal_part
)

# TABLA AVERIAS WORD
def preprocess_df_word_datos_averias(df_word_datos_averias):
    df_word_datos_averias = df_word_datos_averias.rename(columns={'Número de ticket':'nro_incidencia'})
    df_word_datos_averias['nro_incidencia'] = df_word_datos_averias['nro_incidencia'].astype(str)
    df_word_datos_averias = df_word_datos_averias.rename(columns={'Tiempo\nTotal (HH:MM)': 'Tiempo Total (HH:MM)'})
    df_word_datos_averias['Fecha y Hora Inicio'] = pd.to_datetime(df_word_datos_averias['Fecha y Hora Inicio'], format='%d/%m/%Y %H:%M', errors='coerce')
    df_word_datos_averias['Fecha y Hora Fin'] = pd.to_datetime(df_word_datos_averias['Fecha y Hora Fin'], format='%d/%m/%Y %H:%M', errors='coerce')
    return df_word_datos_averias

def preprocess_df_word_telefonia_averias(df_word_telefonia_averias):
    df_word_telefonia_averias = df_word_telefonia_averias.rename(columns={'Número de ticket':'nro_incidencia'})
    df_word_telefonia_averias['nro_incidencia'] = df_word_telefonia_averias['nro_incidencia'].astype(str)
    df_word_telefonia_averias = df_word_telefonia_averias.rename(columns={'Tiempo\nTotal (HH:MM)': 'Tiempo Total (HH:MM)'})
    df_word_telefonia_averias['Fecha y Hora Inicio'] = pd.to_datetime(df_word_telefonia_averias['Fecha y Hora Inicio'], format='%d/%m/%Y %H:%M', errors='coerce')
    df_word_telefonia_averias['Fecha y Hora Fin'] = pd.to_datetime(df_word_telefonia_averias['Fecha y Hora Fin'], format='%d/%m/%Y %H:%M', errors='coerce')
    return df_word_telefonia_averias


#INFORME TECNICO WORD
def preprocess_df_word_datos_informe_tecnico(df_word_datos_informe_tec):
    df_word_datos_informe_tec = df_word_datos_informe_tec.rename(columns={'ticket':'nro_incidencia'})
    df_word_datos_informe_tec['nro_incidencia'] = df_word_datos_informe_tec['nro_incidencia'].astype(str)
    # df_word_datos_informe_tec['Fecha y Hora Inicio'] = pd.to_datetime(df_word_datos_informe_tec['Fecha y Hora Inicio'], format='%Y-%m-%d', errors='coerce')
    # df_word_datos_informe_tec['Fecha y Hora Fin'] = pd.to_datetime(df_word_datos_informe_tec['Fecha y Hora Fin'], format='%Y-%m-%d', errors='coerce')
    return df_word_datos_informe_tec

def preprocess_df_word_telefonia_informe_tecnico(df_word_telefonia_informe_tec): 
    df_word_telefonia_informe_tec = df_word_telefonia_informe_tec.rename(columns={'ticket':'nro_incidencia'})
    df_word_telefonia_informe_tec['nro_incidencia'] = df_word_telefonia_informe_tec['nro_incidencia'].astype(str)
    # df_word_telefonia_informe_tec['Fecha y Hora Inicio'] = pd.to_datetime(df_word_telefonia_informe_tec['Fecha y Hora Inicio'], format='%Y-%m-%d', errors='coerce')
    # df_word_telefonia_informe_tec['Fecha y Hora Fin'] = pd.to_datetime(df_word_telefonia_informe_tec['Fecha y Hora Fin'], format='%Y-%m-%d', errors='coerce')

    
    return df_word_telefonia_informe_tec

# ANEXOS INDISPONIBILIDAD WORD
def preprocess_df_word_datos_anexos_indis(df_word_datos_anexos_indis):
    df_word_datos_anexos_indis = df_word_datos_anexos_indis.rename(columns={'ticket':'nro_incidencia'})
    df_word_datos_anexos_indis['nro_incidencia'] = df_word_datos_anexos_indis['nro_incidencia'].astype(str)
 

    return df_word_datos_anexos_indis

def preprocess_df_word_telefonia_anexos_indis(df_word_telefonia_anexos_indis): 
    df_word_telefonia_anexos_indis = df_word_telefonia_anexos_indis.rename(columns={'ticket':'nro_incidencia'})
    df_word_telefonia_anexos_indis['nro_incidencia'] = df_word_telefonia_anexos_indis['nro_incidencia'].astype(str)
    return df_word_telefonia_anexos_indis


# SGA DINAMICO 335 
def preprocess_335(df_sga_dinamico_335):
    df_sga_dinamico_335['interrupcion_inicio'] = pd.to_datetime(df_sga_dinamico_335['interrupcion_inicio'], errors='coerce', dayfirst=True)
    df_sga_dinamico_335['interrupcion_fin'] = pd.to_datetime(df_sga_dinamico_335['interrupcion_fin'], errors='coerce', dayfirst=True)
    df_sga_dinamico_335['fecha_comunicacion_cliente'] = pd.to_datetime(df_sga_dinamico_335['fecha_comunicacion_cliente'], errors='coerce', dayfirst=True)
    df_sga_dinamico_335['fecha_generacion'] = pd.to_datetime(df_sga_dinamico_335['fecha_generacion'], errors='coerce', dayfirst=True)
    df_sga_dinamico_335['fg_padre'] = pd.to_datetime(df_sga_dinamico_335['fg_padre'], errors='coerce', dayfirst=True)
    df_sga_dinamico_335['hora_sistema'] = pd.to_datetime(df_sga_dinamico_335['hora_sistema'], errors='coerce', dayfirst=True)
    df_sga_dinamico_335["cid"] = df_sga_dinamico_335["cid"].astype(str).fillna("")
    df_sga_dinamico_335['nro_incidencia'] = df_sga_dinamico_335['nro_incidencia'].astype(str)
    df_sga_dinamico_335 = handle_null_values(df_sga_dinamico_335)
    df_sga_dinamico_335["it_determinacion_de_la_causa"] = df_sga_dinamico_335["it_determinacion_de_la_causa"].astype(str).str.strip().fillna('No disponible')
    df_sga_dinamico_335["tipo_caso"] = df_sga_dinamico_335["tipo_caso"].astype(str).str.strip().fillna('No disponible')
    df_sga_dinamico_335["cid"] = df_sga_dinamico_335["cid"].astype(str).str.strip().fillna('No disponible')
    df_sga_dinamico_335 = cut_decimal_part(df_sga_dinamico_335, 'codincidencepadre')
    df_sga_dinamico_335["codincidencepadre"] = df_sga_dinamico_335["codincidencepadre"].astype(str).str.strip().fillna('No disponible')
    return df_sga_dinamico_335

def preprocess_380(df_sga_dinamico_380):
    df_sga_dinamico_380['startdate'] = pd.to_datetime(df_sga_dinamico_380['startdate'],  errors='coerce', dayfirst=True)
    df_sga_dinamico_380['enddate'] = pd.to_datetime(df_sga_dinamico_380['enddate'],  errors='coerce', dayfirst=True)
    df_sga_dinamico_380 = handle_null_values(df_sga_dinamico_380)
    return df_sga_dinamico_380

def preprocess_corte_excel(df_corte_excel):
    df_corte_excel = cut_decimal_part(df_corte_excel,'CUISMP')
    df_corte_excel["CODINCIDENCEPADRE"] = df_corte_excel["CODINCIDENCEPADRE"].astype(str).str.strip().fillna('No disponible')
    df_corte_excel = handle_null_values(df_corte_excel)
    df_corte_excel = df_corte_excel.rename(columns={'TICKET':'nro_incidencia'})
    df_corte_excel['nro_incidencia'] = df_corte_excel['nro_incidencia'].astype(str)
    df_corte_excel['DF'] = df_corte_excel['DF'].astype(str).str.strip().fillna('No disponible')
    df_corte_excel['CUISMP'] = df_corte_excel['CUISMP'].astype(str).str.strip().fillna('No disponible')
    df_corte_excel['DETERMINACIÓN DE LA CAUSA'] = df_corte_excel['DETERMINACIÓN DE LA CAUSA'].astype(str).str.strip().fillna("No disponible")
    df_corte_excel['TIPO CASO'] = df_corte_excel['TIPO CASO'].astype(str).str.strip().fillna("No disponible")
    df_corte_excel['CID'] = df_corte_excel['CID'].astype(str).str.strip().fillna("No disponible")
    df_corte_excel['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'] = df_corte_excel['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'].astype(str).str.strip().fillna("No disponible")
    df_corte_excel['FECHA Y HORA INICIO'] = pd.to_datetime(df_corte_excel['FECHA Y HORA INICIO'], format='%Y-%m-%d', errors='coerce')
    df_corte_excel['FECHA Y HORA FIN'] = pd.to_datetime(df_corte_excel['FECHA Y HORA FIN'], format='%Y-%m-%d', errors='coerce')
    df_corte_excel['TIEMPO (HH:MM)_trimed'] = df_corte_excel['TIEMPO (HH:MM)'].apply(
        lambda x: str(x)[:5] if isinstance(x, str) and x.endswith(":00") else x
    )

    pat = re.compile(
        r'^(?:(?P<days>\d+)\s+days?,\s*)?'   
        r'(?P<hours>\d+):(?P<minutes>\d{2})'  
        r'(?:[:]\d{2})?'                    
        r'\.?$'                            
    )

    def to_hhmm(x):
        s = str(x).strip()
        m = pat.match(s)
        if not m:
            return pd.NA 
        days = int(m.group('days')) if m.group('days') else 0
        hrs  = int(m.group('hours'))
        mins = int(m.group('minutes'))
        total_h = days * 24 + hrs
        return f"{total_h:02d}:{mins:02d}"


    df_corte_excel['FIN-INICIO (HH:MM)_trimed'] = df_corte_excel['FIN-INICIO (HH:MM)'].apply(to_hhmm)

    df_corte_excel['FECHA_Y_HORA_INICIO_fmt'] = (
        df_corte_excel['FECHA Y HORA INICIO']
        .dt.strftime('%d/%m/%Y %H:%M')
        .fillna("N/A")
        .astype(str)
    )

    df_corte_excel['FECHA_Y_HORA_FIN_fmt'] = (
        df_corte_excel['FECHA Y HORA FIN']
        .dt.strftime('%d/%m/%Y %H:%M')
        .fillna("N/A")
        .astype(str)
    )

  
    return df_corte_excel

def preprocess_df_cid_cuismp_sharepoint(df_cid_cuismp_sharepoint):
    df_cid_cuismp_sharepoint = cut_decimal_part(df_cid_cuismp_sharepoint, 'CUISMP')
    df_cid_cuismp_sharepoint = df_cid_cuismp_sharepoint.rename(columns={"CID":"cid"})
    df_cid_cuismp_sharepoint["cid"] = df_cid_cuismp_sharepoint["cid"].astype(str).fillna("")
    df_cid_cuismp_sharepoint["Distrito Fiscal"] = df_cid_cuismp_sharepoint["Distrito Fiscal"].astype(str).str.strip().fillna('No disponible')
    df_cid_cuismp_sharepoint["CUISMP"] = df_cid_cuismp_sharepoint["CUISMP"].astype(str).str.strip().fillna('No disponible')
    df_cid_cuismp_sharepoint["SEDE"] = df_cid_cuismp_sharepoint["SEDE"].astype(str).str.strip().fillna('No disponible')
    return df_cid_cuismp_sharepoint

