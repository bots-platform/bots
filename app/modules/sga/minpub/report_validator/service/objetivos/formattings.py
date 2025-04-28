
from typing import List, Dict
import pandas as pd
from datetime import datetime, timedelta

from app.modules.sga.minpub.report_validator.service.objetivos.decorators import ( 
    log_exceptions
)


@log_exceptions
def _format_interval(dt_start, dt_end) -> str:
    return (
        f"{dt_start.strftime('%d/%m/%Y %H:%M')} "
        f"hasta el día {dt_end.strftime('%d/%m/%Y %H:%M')}"
    )


@log_exceptions
def make_paragraph_paradas_cliente(stops_list):

        if not stops_list:
            return ""
    
        lines = [_format_interval(s['start'], s['end']) for s in stops_list]
        
        total = sum((s['end'] - s['start'] for s in stops_list), timedelta())
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


@log_exceptions
def make_paragraph_paradas_cliente_header(stops_list):

        if not stops_list:
            return ""
    
        lines = [_format_interval(s['start'], s['end']) for s in stops_list]
        
        total = sum((s['end'] - s['start'] for s in stops_list), timedelta())
        hh, rem = divmod(int(total.total_seconds()), 3600)
        mm = rem // 60
        total_str = f"{hh:02d}:{mm:02d}"

        header = (
            "Se tuvo indisponibilidad por parte del cliente "
            "para continuar los trabajos el/los día(s)"
        )
        body = "\n".join(lines)
        footer = f"(Total de horas sin acceso a la sede: {total_str} horas)"
        return f"{header}"

@log_exceptions
def make_paragraph_paradas_cliente_periodos(stops_list):

        if not stops_list:
            return ""
    
        lines = [_format_interval(s['start'], s['end']) for s in stops_list]
        
        total = sum((s['end'] - s['start'] for s in stops_list), timedelta())
        hh, rem = divmod(int(total.total_seconds()), 3600)
        mm = rem // 60
        total_str = f"{hh:02d}:{mm:02d}"

        header = (
            "Se tuvo indisponibilidad por parte del cliente "
            "para continuar los trabajos el/los día(s)"
        )
        body = "\n".join(lines)
        footer = f"(Total de horas sin acceso a la sede: {total_str} horas)"
        return f"{body}"


@log_exceptions
def make_paragraph_paradas_cliente_footer(stops_list):

        if not stops_list:
            return ""
    
        lines = [_format_interval(s['start'], s['end']) for s in stops_list]
        
        total = sum((s['end'] - s['start'] for s in stops_list), timedelta())
        hh, rem = divmod(int(total.total_seconds()), 3600)
        mm = rem // 60
        total_str = f"{hh:02d}:{mm:02d}"

        header = (
            "Se tuvo indisponibilidad por parte del cliente "
            "para continuar los trabajos el/los día(s)"
        )
        body = "\n".join(lines)
        footer = f"(Total de horas sin acceso a la sede: {total_str} horas)"
        return f"{footer}"