
import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime, timedelta

import re


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
            raise
    return wrapper


def resolve_clock_stop_overlaps(clock_stops: List[Dict]) -> List[Dict]:
    """
    Merge overlapping or back‑to‑back clock stops by nro_incidencia.
    """
    if not clock_stops:
        return []

    incidents: Dict[str, List[Dict]] = {}
    for stop in clock_stops:
        key = stop.get('nro_incidencia', 'unknown')
        incidents.setdefault(key, []).append(stop)

    resolved_all: List[Dict] = []
    for stops in incidents.values():
        stops = sorted(stops, key=lambda x: x['start'])
        for i, s in enumerate(stops):
            if pd.isna(s['end']):
                if i + 1 < len(stops) and not pd.isna(stops[i+1]['start']):
                    s['end'] = stops[i+1]['start']
                else:
                    s['end'] = None
        valid = [s for s in stops if s['end'] is not None]
        if not valid:
            continue

        merged = [valid[0]]
        for cur in valid[1:]:
            last = merged[-1]
            if cur['start'] <= last['end']:
                last['end'] = max(last['end'], cur['end'])
            else:
                merged.append(cur)
        resolved_all.extend(merged)

    return resolved_all


def _format_interval(dt_start, dt_end) -> str:
    return (
        f"{dt_start.strftime('%d/%m/%Y %H:%M')} "
        f"hasta el día {dt_end.strftime('%d/%m/%Y %H:%M')}"
    )


@log_exceptions
def validate_indisponibilidad(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds an 'expected_indisponibilidad' text from clock_stops (merging overlaps)
    and compares it to the user‑entered INDISPONIBILIDAD column.
    Adds columns:
      - expected_indisponibilidad (str)
      - indisponibilidad_ok (bool)
      - Validation_OK (bool)
      - fail_count (0/1)
    """
    df = df.copy()

    def make_expected(stops_list):
        stops = resolve_clock_stop_overlaps(stops_list or [])
        if not stops:
            return ""
    
        lines = [_format_interval(s['start'], s['end']) for s in stops]
        
        total = sum((s['end'] - s['start'] for s in stops), timedelta())
        hh, rem = divmod(int(total.total_seconds()), 3600)
        mm = rem // 60
        total_str = f"{hh:02d}:{mm:02d}"

        header = (
            "Se tuvo indisponibilidad por parte del cliente "
            "para continuar los trabajos el/los día(s)"
        )
        body = "\n".join(lines)
        footer = f"(Total de horas sin acceso a la sede: {total_str} horas)"
        return f"{header}\n{body}\n{footer}"

    df['expected_indisponibilidad'] = df['clock_stops'].apply(make_expected)
    df['indisponibilidad_ok'] = (
        df['INDISPONIBILIDAD'].astype(str).str.strip()
        == df['expected_indisponibilidad']
    )

    df['Validation_OK'] = df['indisponibilidad_ok']
    df['fail_count']   = (~df['Validation_OK']).astype(int)
    return df

