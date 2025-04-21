
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


