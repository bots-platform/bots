

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)
from app.modules.sga.minpub.report_validator.service.objetivos.utils.validations import validate_required_columns_from_excel
import pandas as pd

@log_exceptions
def extract_corte_excel(path_corte_excel, skipfooter):

    required_columns = [
            'TICKET',  
            'FECHA Y HORA INICIO', 
            'FECHA Y HORA FIN', 
            'CUISMP',  
            'TIPO CASO',  
            'AVERÍA', 
            'TIEMPO (HH:MM)',  
            'COMPONENTE', 
            'DF',  
            'OBSERVACIÓN', 
            'CID', 
            'FIN-INICIO (HH:MM)',  
            'DETERMINACIÓN DE LA CAUSA',  
            'RESPONSABILIDAD', 
            'TIPO REPORTE',
            'Duracion entero',
            'Agrupación entero',
            'CODINCIDENCEPADRE',
            'MASIVO',
            'MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS',
            'TIPO DE INCIDENCIA',
            'TIEMPO INTERRUPCION',
            'INDISPONIBILIDAD'
        ]
    
    try:
        df = validate_required_columns_from_excel(path_corte_excel, required_columns, skipfooter)
        return df
    except Exception as e:
        # Si hay error, crear un DataFrame vacío con las columnas requeridas
        print(f"Error al procesar archivo Excel: {e}")
        return pd.DataFrame(columns=required_columns)


