from pywinauto import Application, Desktop
import logging
from time import sleep
from app.modules.sga.scripts.sga_navigation import navegar_sistema_CMR, seleccionar_opcion_sga

def connect_to_sga():
    try:
        logging.info("Verificando si la aplicación SGA está abierta...")
        app = Application(backend="uia").connect(title_re=".*SGA -")
        navegacion_window = app.window(title_re=".*SGA -")

        navegacion_window.set_focus()
        if not navegacion_window.is_maximized():
            navegacion_window.maximize()
            logging.info("Ventana maximizada.")
        else:
            logging.info("La ventana ya está maximizada.")
            
        sleep(1)
        logging.info("Conexión exitosa con la aplicación SGA.")

        return navegacion_window
    
    
    except Exception as e:
        logging.error(f"No se pudo conectar a la aplicación SGA: {e}")
        raise Exception("La aplicación SGA no está abierta o no está logueada. Por favor, verifica e inténtalo de nuevo.")

def connect_to_atencionCliente_window():
    try:
        logging.error(f"Trying to identify SGA Atencion al cliente window")
        atencion_window = Desktop(backend="uia").window(title_re="SGA Atención al Cliente.*")
        logging.info("Ventana principal SGA Atención al Cliente")
        return atencion_window

    except Exception as e:
        logging.error(f"Error al identificar la ventana principal de SGA Atención al cliente")
        raise   

def close_atencionCliente_window(atencion_window):
    try:
        logging.info("Intentando cerrar la ventana de operaciones...")

        if atencion_window.exist() and atencion_window.is_visible():
            atencion_window.close()
            logging.info("Ventana  de operaciones cerrada exitosamente")
        else:
            logging.warning("La ventana de atencion al cliente no esta visible o no existe")
    
    except Exception as e:
        logging.error(f"Error al intentar cerrar la ventanta de operaciones: {e}")
        raise

def send_alert():
    try:
        navegation_window = connect_to_sga()
        navegar_sistema_CMR(navegation_window)
        seleccionar_opcion_sga(navegation_window, "SGA Atención al Cliente")
        sleep(10)

    except Exception as e:
        error_message = f"Error to send the alert"
        logging.error(error_message)
        raise

