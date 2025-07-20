from typing import List, Dict
import pandas as pd
import re
from datetime import datetime, timedelta
from docx import Document
from typing import Tuple, Optional

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
def calculate_total_clock_stop_minutes_by_incidencia(
    nro_incidencia:str,
    interruption_start: datetime,
    interruption_end: datetime,
    df_sga_paradas: pd.DataFrame
    ) -> float:
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
        #logger.info(f"No clock stops found for incident {nro_incidencia}")
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
def calculate_total_clock_stop_by_incidencia(
    nro_incidencia:str,
    interruption_start: datetime,
    interruption_end: datetime,
    df_sga_paradas: pd.DataFrame
    ) -> float:
    """
    Calculate the total clock stops for a ticket, considering constraints.

    Args:
        nro_incidencia: The ticket identifier
        interrupcion_inicio: Start time of the interruption from REPORTE DINAMICO 335 
        interrupcion_fin: End time of the interruption from REPORTE DINAMICO 335 
    
    Returns:
        Total clock stop by tickets
    
    """   
    df_sga_paradas['nro_incidencia'] = df_sga_paradas['nro_incidencia'].astype(str)
    nro_incidencia_stops = df_sga_paradas[df_sga_paradas['nro_incidencia'] == nro_incidencia].copy()

    if nro_incidencia_stops.empty:
        #logger.info(f"No clock stops found for incident {nro_incidencia}")
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
        r'\b(A través)\b.*\b\1\b',
    ]
    return any(re.search(p, text) for p in patterns )
   

@log_exceptions
def count_A_traves_mayus(text: str) -> int:
    """
    Devuelve el número de veces que aparece 'A través' (con A mayúscula) en el texto.
    """
    if not isinstance(text, str):
        return 0
    matches = re.findall(r'A\s+través', text)
    return len(matches)

@log_exceptions
def has_multiple_A_traves_mayus(text: str) -> bool:
    """
    Devuelve True si 'A través' (con A mayúscula) aparece más de una vez en el texto.
    """
    return count_A_traves_mayus(text) > 1

@log_exceptions
def has_cliente_debido_error(text: str) -> bool:
    """
    Returns True if the text contains variations of "cliente/debido", which are considered writing errors.
    The variations checked are: "cliente/ debido", "cliente / debido", "cliente /debido", "cliente/debido".
    The check is case-insensitive.
    """
    if not isinstance(text, str):
        return False
    pattern = re.compile(r'cliente\s*/\s*debido', re.IGNORECASE)
    return bool(pattern.search(text))


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
    print(f"DEBUG Original: Líneas seleccionadas - Inicio: '{start_line}' | Fin: '{end_line}'")

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
    print(inicio, fin)
    return (inicio, fin) if inicio and fin else (None, None)



# patrón que acepta "DD/MM/YYYY" + opcional "a las" + "HH:MM"
_fecha_hora_pat = re.compile(
    r'(\d{2}/\d{2}/\d{4})\s*(?:a las\s*)?(\d{1,2}:\d{2})',
    flags=re.IGNORECASE
)

@log_exceptions
def extract_date_range_last_simple(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Devuelve (fecha_hora_inicio, fecha_hora_fin) tomando
    las dos últimas ocurrencias 'DD/MM/YYYY hh:mm' del texto.
    Si no hay al menos dos, retorna (None, None).
    """
    if not isinstance(text, str) or not text.strip():
        return (None, None)

    # busca todas las parejas (fecha, hora) en el texto
    matches = _fecha_hora_pat.findall(text)
    if len(matches) < 2:
        return (None, None)

    # agarra las dos últimas
    start_fecha, start_hora = matches[-2]
    end_fecha,   end_hora   = matches[-1]

    # devuelve crudo, tal cual aparecen
    return (f"{start_fecha} {start_hora}", f"{end_fecha} {end_hora}")


@log_exceptions
def extract_date_range_last_normalized(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Devuelve (fecha_hora_inicio, fecha_hora_fin) tomando
    las dos últimas ocurrencias 'DD/MM/YYYY hh:mm' del texto,
    y las normaliza al formato 'DD/MM/YYYY HH:MM' con ceros a la izquierda.
    Si no hay al menos dos, retorna (None, None).
    """
    if not isinstance(text, str) or not text.strip():
        return (None, None)

    # busca todas las parejas (fecha, hora) en el texto
    matches = _fecha_hora_pat.findall(text)
    if len(matches) < 2:
        return (None, None)

    def normalize(fecha: str, hora: str) -> str:
        d, m, y = fecha.split('/')
        h, mi = hora.split(':')
        return f"{d.zfill(2)}/{m.zfill(2)}/{y} {h.zfill(2)}:{mi.zfill(2)}"

    # agarra las dos últimas
    f1, t1 = matches[-2]
    f2, t2 = matches[-1]

    return (normalize(f1, t1), normalize(f2, t2))



@log_exceptions
def extract_date_range_body(text: str) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(text, str):
        return (None, None)

    lines = text.splitlines()
    meta_pat = re.compile(r'(?i)^fecha y hora(?: de)? (?:inicio|fin)\s*:?')

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



@log_exceptions
def extract_date_range_body_v2(text: str) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(text, str):
        return (None, None)

    lines = text.splitlines()
    meta_pat = re.compile(r'(?i)^fecha y hora(?: de)? (?:inicio|fin)\s*:?')

    while lines and (not lines[-1].strip() or meta_pat.match(lines[-1].strip())):
        lines.pop()
    clean_body = "\n".join(lines)

    # Separate patterns for date and time
    fecha_pat = re.compile(r'(\d{2}/\d{2}/\d{4})', re.IGNORECASE)
    hora_pat = re.compile(r'(\d{1,2}:\d{2})', re.IGNORECASE)

    def _normalize(fecha: str, hora: str) -> str:
        d, m, y = fecha.split('/')
        h, mm = hora.split(':')
        return f"{d.zfill(2)}/{m.zfill(2)}/{y} {h.zfill(2)}:{mm.zfill(2)}"

    midpoint = len(clean_body) / 2
    start_date = None
    end_date = None

    # Find all dates and times with their positions
    fecha_matches = [(m.group(1), m.start()) for m in fecha_pat.finditer(clean_body)]
    hora_matches = [(m.group(1), m.start()) for m in hora_pat.finditer(clean_body)]

    if not fecha_matches or not hora_matches:
        return ("No disponible", "No disponible")

    # Sort all matches by their position in text
    all_matches = sorted(fecha_matches + hora_matches, key=lambda x: x[1])
    
    # Process matches in order of appearance
    current_fecha = None
    current_hora = None
    
    for value, pos in all_matches:
        if fecha_pat.match(value):
            current_fecha = value
        elif hora_pat.match(value):
            current_hora = value
            
        # If we have both a date and time, create the full date
        if current_fecha and current_hora:
            full_date = _normalize(current_fecha, current_hora)
            
            if pos < midpoint and not start_date:
                start_date = full_date
            elif pos >= midpoint:
                end_date = full_date
                
            # Reset current values after using them
            current_fecha = None
            current_hora = None

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

