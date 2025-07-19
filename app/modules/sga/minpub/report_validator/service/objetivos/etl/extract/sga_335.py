

from app.modules.sga.minpub.report_validator.service.objetivos.utils.decorators import ( 
    log_exceptions
)
from app.modules.sga.minpub.report_validator.service.objetivos.utils.validations import validate_required_columns_from_excel

@log_exceptions
def extract_sga_335(path_sga_dinamico_335):

    required_columns = [
            'nro_incidencia',  # ticket
            'canal_ingreso',  # tipo generacion ticket
            'interrupcion_inicio',  # fecha/hora interrupcion
            'fecha_generacion',  # fecha/hora generacion
            'interrupcion_fin',  # fecha/ hora subsanacion
            'cid',  # CID
            'tipo_caso',  # Tipo Caso
            'tipificacion_problema',  # averia
            'it_determinacion_de_la_causa',  # DETERMINACION DE LA CAUSA
            'it_medidas_tomadas',  # MEDIDAS CORRECTIVAS Y/O PREVENTIVAS TOMADAS
            'it_conclusiones',  # RECOMENDACIONES
            'tiempo_interrupcion',  # tiempo subsanacion efectivo
            'tipificacion_interrupcion',  # tiempo de indisponibilidad
            'tipificacion_tipo',  # ATRIBUIBLE  o RESPONSABLE
            'fecha_comunicacion_cliente',  # Fecha hora solicitud  
            'masivo',
            'codincidencepadre'

        ]
    
    df = validate_required_columns_from_excel(path_sga_dinamico_335, required_columns)
    return df


