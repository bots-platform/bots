
from app.modules.sga.minpub.report_validator.service.objetivos.objetivo_1.objetivo_1 import validation_objetivo_1
from typing import List, Dict
import pandas as pd
from datetime import datetime

from utils.logger_config import get_sga_logger
 
logger = get_sga_logger()

def log_exceptions(func):
    """
    Decorator to log exceptions in a function using the shared 'logger'.
    It will also re-raise the exception so that the caller can handle it
    appropriately (e.g., fail fast or continue).
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as exc:
            logger.error(
                f"Error in function '{func.__name__}': {exc}",
                exc_info=True
            )
            # Optionally, decide whether to re-raise or swallow the exception.
            # Usually best practice is to re-raise so the pipeline can decide what to do:
            raise
    return wrapper

@log_exceptions
def cut_decimal_part(df, column):
    """
    Converts a DataFrame column from float (or numeric string) to a string
    by removing the decimal part (i.e. converting 13.5 to "13", 12.0 to "12").
    Non-numeric values are converted to NaN and then to an empty string.
    """
    df[column] = pd.to_numeric(df[column], errors='coerce')

    df[column] = df[column].apply(lambda x: str(int(x)) if pd.notnull(x) else '')
    
    return df

@log_exceptions
def handle_null_values(df, fill_str="", fill_float=0.0, fill_datetime=""):
    """
    Fill null values in DataFrame columns based on data type.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        fill_str (str): Value to replace nulls in object/string columns. Default is "".
        fill_float (float): Value to replace nulls in float columns. Default is 0.0.
        fill_datetime: Value to replace nulls in datetime columns. 
                       Default is "", but you can also pass a default datetime.
    
    Returns:
        pd.DataFrame: The DataFrame with nulls handled.
    """

    obj_cols = df.select_dtypes(include=['object']).columns
    for col in obj_cols:
        df[col] = df[col].fillna(fill_str).astype(str)
    

    float_cols = df.select_dtypes(include=['float64']).columns
    for col in float_cols:
        df[col] = df[col].fillna(fill_float)
        

    datetime_cols = df.select_dtypes(include=['datetime64[ns]']) 
    for col in datetime_cols:
        df[col] = df[col].fillna(fill_datetime)
        
    return df

@log_exceptions
def resolve_clock_stop_overlaps(clock_stops: List[Dict]) -> List[Dict]:
    """
    Eliminate overlaps in clock stops (paradas de reloj) by nro_incidencia.

    Args:
        clock_stops: List of clock stops with 'start' 'end' datetime and 'nro_incidencia'

    Returns:
        List of non-overlapping clock stops
            
    """
    if not clock_stops:
        return []
    
    incidents = {}
    for stop in clock_stops:
        nro_incidencia = stop.get('nro_incidencia', 'unknown')
        if nro_incidencia not in incidents:
            incidents[nro_incidencia] = []
        incidents[nro_incidencia].append(stop)

    
    resolved_all = []   

    for nro_incidencia, incident_stops in incidents.items():
        sorted_stops = sorted(incident_stops, key=lambda x: x['start'])

        for i, stop in enumerate(sorted_stops):
            if pd.isna(stop['end']):
                if i < len(sorted_stops) - 1 and not pd.isna(sorted_stops[i+1]['start']):
                    stop['end'] = sorted_stops[i+1]['start']
                else:
                    logger.warning(f"Removing stop with missing end date for nro_incidencia {nro_incidencia}")
                    continue
        
        valid_stops = [stop for stop in sorted_stops if not pd.isna(stop['end'])]

        if not valid_stops:
            continue

        resolved_stops = [valid_stops[0]]

        for current_stop in valid_stops[1:]:
            last_resolved = resolved_stops[-1]

            if current_stop['start'] <= last_resolved['end']:
                last_resolved['end'] = max(last_resolved['end'], current_stop['end'])
            else:
                resolved_stops.append(current_stop)

        resolved_all.extend(resolved_stops)

    return resolved_all

@log_exceptions
def calculate_total_clock_stop_minutes(nro_incidencia:str, interruption_start: datetime, interruption_end: datetime, df_sga_paradas: pd.DataFrame) -> float:
    """
    Calculate the total clock minutes for a ticket, considering constraints.

    Args:
        nro_incidencia: The ticket identifier
        interrupcion_inicio: Start time of the interruption from REPORTE DINAMICO 335 
        interrupcion_fin: End time of the interruption from REPORTE DINAMICO 335 
    
    Returns:
        Total clock stop minutes
    
    """   
    df_sga_paradas['nro_incidencia'] = df_sga_paradas['nro_incidencia'].astype(str)
    nro_incidencia_stops = df_sga_paradas[df_sga_paradas['nro_incidencia'] == nro_incidencia].copy()

    if nro_incidencia_stops.empty:
        logger.info(f"No clock stops found for incident {nro_incidencia}")
        return 0.0
    
    clock_stops = []

    for _, stop in nro_incidencia_stops.iterrows():
        start_date = stop.get('startdate')
        end_date = stop.get('enddate')

        if pd.isna(start_date):
            logger.warning(f"Skipping record with missing start date for incident {nro_incidencia}")
            continue

        if start_date < interruption_start:
            logger.info(f"Adjusting start time to interruption en for incident {nro_incidencia}")
            start_date = interruption_start

        if not pd.isna(end_date):
            if end_date > interruption_end:
                logger.info(f"Adjusting end time to interruption en for incident {nro_incidencia}")
                end_date = interruption_end

            if start_date < end_date:
                clock_stops.append({
                    'start': start_date,
                    'end': end_date,
                    'nro_incidencia': nro_incidencia
                })
        else:
            clock_stops.append({
                'start': start_date,
                'end': end_date,
                'nro_incidencia': nro_incidencia
            })
    resolved_stops = resolve_clock_stop_overlaps(clock_stops)

    total_minutes = sum(
        (stop['end'] - stop['start']).total_seconds() / 60
        for stop in resolved_stops
        if not pd.isna(stop['end']) and not pd.isna(stop['start'])
    )
    return total_minutes


@log_exceptions
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

    df_sga_dinamico_380['startdate'] = pd.to_datetime(df_sga_dinamico_380['startdate'],  errors='coerce', dayfirst=True)
    df_sga_dinamico_380['enddate'] = pd.to_datetime(df_sga_dinamico_380['enddate'],  errors='coerce', dayfirst=True)
    df_sga_dinamico_380 = handle_null_values(df_sga_dinamico_380)

    df_corte_excel = cut_decimal_part(df_corte_excel,'CUISMP')
    #df_corte_excel = cut_decimal_part(df_corte_excel,'CODINCIDENCEPADRE')
    df_corte_excel["CODINCIDENCEPADRE"] = df_corte_excel["CODINCIDENCEPADRE"].astype(str).str.strip().fillna('No disponible')
    df_corte_excel = handle_null_values(df_corte_excel)
    df_corte_excel = df_corte_excel.rename(columns={'TICKET':'nro_incidencia'})
    df_corte_excel['nro_incidencia'] = df_corte_excel['nro_incidencia'].astype(str)
    df_corte_excel['DF'] = df_corte_excel['DF'].astype(str).str.strip().fillna('No disponible').str.lower()
    df_corte_excel['CUISMP'] = df_corte_excel['CUISMP'].astype(str).str.strip().fillna('No disponible')
    df_corte_excel['DETERMINACIÓN DE LA CAUSA'] = df_corte_excel['DETERMINACIÓN DE LA CAUSA'].astype(str).str.strip().fillna("No disponible")
    df_corte_excel['TIPO CASO'] = df_corte_excel['TIPO CASO'].astype(str).str.strip().fillna("No disponible")
    df_corte_excel['CID'] = df_corte_excel['CID'].astype(str).str.strip().fillna("No disponible")
    df_corte_excel['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'] = df_corte_excel['MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS'].astype(str).str.strip().fillna("No disponible")
    df_corte_excel['FECHA Y HORA INICIO'] = pd.to_datetime(df_corte_excel['FECHA Y HORA INICIO'], format='%Y-%m-%d', errors='coerce')
    df_corte_excel['FECHA Y HORA FIN'] = pd.to_datetime(df_corte_excel['FECHA Y HORA FIN'], format='%Y-%m-%d', errors='coerce')


    df_cid_cuismp_sharepoint = cut_decimal_part(df_cid_cuismp_sharepoint, 'CUISMP')
    df_cid_cuismp_sharepoint = df_cid_cuismp_sharepoint.rename(columns={"CID":"cid"})
    df_cid_cuismp_sharepoint["cid"] = df_cid_cuismp_sharepoint["cid"].astype(str).fillna("")
    df_cid_cuismp_sharepoint["Distrito Fiscal"] = df_cid_cuismp_sharepoint["Distrito Fiscal"].astype(str).str.strip().fillna('No disponible').str.lower()
    df_cid_cuismp_sharepoint["CUISMP"] = df_cid_cuismp_sharepoint["CUISMP"].astype(str).str.strip().fillna('No disponible')



    @log_exceptions
    def merge_sga_335_corte_excel_sharepoint_cuismp_sga380(
        df_corte_excel: pd.DataFrame, 
        df_sga_dinamico_335: pd.DataFrame,
        df_cid_cuismp_sharepoint: pd.DataFrame,
        df_sga_dinamico_380: pd.DataFrame,
        match_type:str
    ) -> pd.DataFrame:
        """
        Common merge function for Objective 1.

        Merges:
          - corte-excel  with sga_dinamico_335 on 'nro_incidencia'

        Returns a merged DataFrame with common columns needed.
        """

        merged_sga335_excel = pd.merge(
            df_corte_excel,
            df_sga_dinamico_335,
            on='nro_incidencia',
            how='left',
            indicator=True,
            suffixes=('_corte_excel', '_sga_dinamico_335')
        )

        merge_sga_335_corte_excel_matched_with_sharepoint_cid_cuismp = pd.merge(
        merged_sga335_excel,
        df_cid_cuismp_sharepoint,
        on='cid',
        how='left',
        suffixes=('_sga_dinamico_335_excel_matched', '_sharepoint_cid_cuismp')
        )

        merge_sga_335_corte_excel_matched_with_sharepoint_cid_cuismp['sum_paradas'] = merge_sga_335_corte_excel_matched_with_sharepoint_cid_cuismp.apply(
            lambda r: calculate_total_clock_stop_minutes(
                nro_incidencia = r["nro_incidencia"],
                interruption_start = r["interrupcion_inicio"],
                interruption_end = r["interrupcion_fin"],
                df_sga_paradas = df_sga_dinamico_380
            ),
            axis= 1
        )

        matched_rows = merge_sga_335_corte_excel_matched_with_sharepoint_cid_cuismp[merge_sga_335_corte_excel_matched_with_sharepoint_cid_cuismp['_merge'] == match_type]

        return matched_rows
      
        


    df_matched_corte_sga335_Sharepoint_cuismp_sga380 = merge_sga_335_corte_excel_sharepoint_cuismp_sga380(
        df_corte_excel, df_sga_dinamico_335,
        df_cid_cuismp_sharepoint, df_sga_dinamico_380,
        'both'
        )
    df_unmatched_corte_sga335_Sharepoint_cuismp_sga380 = merge_sga_335_corte_excel_sharepoint_cuismp_sga380(
        df_corte_excel,
        df_sga_dinamico_335,
        df_cid_cuismp_sharepoint,
        df_sga_dinamico_380,
        'left_only'
        )


    obj1_df = validation_objetivo_1( df_matched_corte_sga335_Sharepoint_cuismp_sga380, df_unmatched_corte_sga335_Sharepoint_cuismp_sga380)

    results.extend(obj1_df.to_dict(orient='records'))
    
    return results