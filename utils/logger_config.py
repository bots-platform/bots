import logging
import os

def setup_logger(logger_name, log_file):
    """Configura y retorna un logger personalizado"""
    
    # Crear el directorio si no existe
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # Crear y configurar el logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    
    # Evitar duplicación de handlers
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger

# Configuraciones predefinidas para diferentes módulos
def get_sga_logger():
    return setup_logger('sga', 'logs/sga/sga.log')

def get_oplogin_logger():
    return setup_logger('oplogin', 'logs/oplogin/oplogin.log')

def get_newcallcenter_logger():
    return setup_logger('newcallcenter', 'logs/newcallcenter/newcallcenter.log')

def get_semaforo_logger():
    return setup_logger('semaforo', 'logs/semaforo/semaforo.log')

def get_reporteCombinado_logger():
    return setup_logger('reportes_combinados', 'logs/reportes_combinados/reportes_combinados.log')

def get_sharepoint_HorarioGeneralATCORP_logger():
    return setup_logger('sharepointHorarioGeneralATCORP', 'logs/sharepoint/HorarioGeneralATCORP.log')

def get_sharepoint_HorarioMesaATCORP():
    return setup_logger('sharepointHorarioMesaATCORP', 'logs/sharepoint/HorarioMesaATCORP.log')