from typing import List, Dict
import re
from datetime import datetime, timedelta
from docx import Document
from typing import Tuple, Optional
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, when, max as spark_max

# import language_tool_python
# _tool_es = language_tool_python.LanguageTool('es')

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)
from utils.logger_config import get_sga_logger
logger = get_sga_logger()


#PARADAS DE RELOJ
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
            if stop['end'] is None:
                if i < len(sorted_stops) - 1 and sorted_stops[i+1]['start'] is not None:
                    stop['end'] = sorted_stops[i+1]['start']
                else:
                    logger.warning(f"Removing stop with missing end date for nro_incidencia {nro_incidencia}")
                    continue
        
        valid_stops = [stop for stop in sorted_stops if stop['end'] is not None]

        if not valid_stops:
            continue

        resolved_stops = [valid_stops[0]]

        for current_stop in valid_stops[1:]:
            last_resolved = resolved_stops[-1]

            if current_stop['start'] <= last_resolved['end']:
                last_resolved['end'] = spark_max(last_resolved['end'], current_stop['end'])
            else:
                resolved_stops.append(current_stop)

        resolved_all.extend(resolved_stops)

    return resolved_all

@log_exceptions
def calculate_total_clock_stop_minutes_by_incidencia(
    nro_incidencia: str,
    interruption_start: datetime,
    interruption_end: datetime,
    df_sga_paradas
    ) -> float:
    """
    Calculate the total clock minutes for a ticket, considering constraints.

    Args:
        nro_incidencia: The ticket identifier
        interruption_start: Start time of the interruption from REPORTE DINAMICO 335 
        interruption_end: End time of the interruption from REPORTE DINAMICO 335 
        df_sga_paradas: PySpark DataFrame containing the stops data
    
    Returns:
        Total clock stop minutes
    
    """   
    # Convert nro_incidencia to string and filter
    df_sga_paradas = df_sga_paradas.withColumn('nro_incidencia', F.col('nro_incidencia').cast(StringType()))
    nro_incidencia_stops = df_sga_paradas.filter(F.col('nro_incidencia') == nro_incidencia)

    if nro_incidencia_stops.count() == 0:
        logger.info(f"No clock stops found for incident {nro_incidencia}")
        return 0.0
    
    # Convert to list of dictionaries for processing
    clock_stops = []
    for row in nro_incidencia_stops.collect():
        start_date = row.get('startdate')
        end_date = row.get('enddate')

        if start_date is None:
            logger.warning(f"Skipping record with missing start date for incident {nro_incidencia}")
            continue

        if start_date < interruption_start:
            logger.info(f"Adjusting start time to interruption en for incident {nro_incidencia}")
            start_date = interruption_start

        if end_date is not None:
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
        if stop['end'] is not None and stop['start'] is not None
    )
    return total_minutes

@log_exceptions
def calculate_total_clock_stop_by_incidencia(
    nro_incidencia: str,
    interruption_start: datetime,
    interruption_end: datetime,
    df_sga_paradas
    ) -> List[Dict]:
    """
    Calculate the total clock stops for a ticket, considering constraints.

    Args:
        nro_incidencia: The ticket identifier
        interruption_start: Start time of the interruption from REPORTE DINAMICO 335 
        interruption_end: End time of the interruption from REPORTE DINAMICO 335 
        df_sga_paradas: PySpark DataFrame containing the stops data
    
    Returns:
        List of clock stops for the ticket
    """   
    # Convert nro_incidencia to string and filter
    df_sga_paradas = df_sga_paradas.withColumn('nro_incidencia', F.col('nro_incidencia').cast(StringType()))
    nro_incidencia_stops = df_sga_paradas.filter(F.col('nro_incidencia') == nro_incidencia)

    if nro_incidencia_stops.count() == 0:
        logger.info(f"No clock stops found for incident {nro_incidencia}")
        return []
    
    # Convert to list of dictionaries for processing
    clock_stops = []
    for row in nro_incidencia_stops.collect():
        start_date = row.get('startdate')
        end_date = row.get('enddate')

        if start_date is None:
            logger.warning(f"Skipping record with missing start date for incident {nro_incidencia}")
            continue

        if start_date < interruption_start:
            logger.info(f"Adjusting start time to interruption en for incident {nro_incidencia}")
            start_date = interruption_start

        if end_date is not None:
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
    return resolved_stops


@log_exceptions
def has_repetition(text: str) -> bool:
    """
    Returns True if text contains o forbiden repetition of:
    - "Inmediatamente" twice, or
    - "A través" twice.
    """
    if not isinstance(text, str):
        return False
    
    patterns = [
        r'(?i)\b(Inmediatamente, claro revisó)\b.*\b\1\b',
        r'(?i)\b(A través de los Sistemas)\b.*\b\1\b',
    ]
    return any(re.search(p, text) for p in patterns)
   

# EXTRACT RANGE DATOS FECHA INICIO Y FIN FROM INDISPONIBILIDAD EXCEL Y SGA 



@log_exceptions
def extract_date_range_last(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Devuelve (fecha_hora_inicio, fecha_hora_fin) extraídas
    de las dos últimas líneas que contengan un
    'DD/MM/YYYY hh:mm' (con o sin 'a las'/'horas').
    Si falla el parseo, retorna (None, None).
    """
    if not isinstance(text, str):
        return (None, None)

    normalized = text.replace('\r', ' ')

    lines = [ln.strip() for ln in normalized.splitlines() if ln.strip()]
    
    if len(lines) < 2:
        return (None, None)

    # Captura DD/MM/YYYY seguido de HH:MM, separados por cualquier cosa
    fecha_hora_pat = re.compile(
    r'(\d{2}/\d{2}/\d{4})\D{0,10}(\d{1,2}:\d{2})',
    flags=re.IGNORECASE
    )


    date_lines = [ln for ln in lines if fecha_hora_pat.search(ln)]
    if len(date_lines) < 2:
        return (None, None)

    start_line, end_line = date_lines[-2], date_lines[-1]

    def _normalize(fecha: str, hora: str) -> str:
        d, m, y = fecha.split('/')
        h, mm = hora.split(':')
        d = d.zfill(2)
        m = m.zfill(2)
        h = h.zfill(2)
        mm = mm.zfill(2)
        return f"{d}/{m}/{y} {h}:{mm}"

    def parse(line: str) -> Optional[str]:
        m = fecha_hora_pat.search(line)
        # return f"{m.group(1)} {m.group(2)}" if m else None
        fecha = m.group(1)
        hora = m.group(2)
        fecha_hora_normzalize = _normalize(fecha, hora)
        return fecha_hora_normzalize

    inicio = parse(start_line)
    fin    = parse(end_line)
    
    return (inicio, fin) if inicio and fin else (None, None)



@log_exceptions
def extract_date_range_body(text: str) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(text, str):
        return (None, None)

    lines = text.splitlines()
    meta_pat = re.compile(r'(?i)^fecha y hora(?: de)? (?:inicio|fin)\s*:')

    while lines and (not lines[-1].strip() or meta_pat.match(lines[-1].strip())):
        lines.pop()
    clean_body = "\n".join(lines)

    fecha_hora_pat = re.compile(
        r'(\d{2}/\d{2}/\d{4})\D{0,10}(\d{1,2}:\d{2})',
        flags=re.IGNORECASE
    )

    def _normalize(fecha: str, hora: str) -> str:
        d, m, y = fecha.split('/')
        h, mm = hora.split(':')
        return f"{d.zfill(2)}/{m.zfill(2)}/{y} {h.zfill(2)}:{mm.zfill(2)}"

    midpoint = len(clean_body) / 2
    start_date = None
    end_date = None

    for match in fecha_hora_pat.finditer(clean_body):
        fecha, hora = match.groups()
        full_date = _normalize(fecha, hora)
        pos = match.start()

        if pos < midpoint and not start_date:
            start_date = full_date
        elif pos >= midpoint:
            end_date = full_date

    return (
        start_date if start_date else "No disponible",
        end_date if end_date else "No disponible"
    )



# @log_exceptions
# def is_langtool_clean(text:str) -> bool:

#     """
#     Return True if LanguageTool reports zero issues in the text.
#     """
#     if not isinstance(text, str) or not text.strip():
#         return True
    
#     matches = _tool_es.check(text)
#     return len(matches) == 0

