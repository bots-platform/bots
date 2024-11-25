from http.client import HTTPException
from time import sleep
from pywinauto import Application, Desktop
import os
import logging
from dotenv import load_dotenv

from app.modules.sga.scripts.sga_navigation import navegar_sistema_tecnico, seleccionar_opcion_sga
from app.modules.sga.scripts.sga_operations import (
    seleccionar_control_de_tareas,
    seleccionar_atcorp,
    abrir_reporte_dinamico,
    seleccionar_275_data_previa,
    seleccionar_fecha_secuencia,
    seleccionar_clipboard,
    select_column_codiIncidencia,
    seleccionar_276_averias,
    seleccionar_checkbox_nroincidencias,
    cerrar_reporte_Dinamico,
    click_button_3puntos,
    seleccion_multiple_listado,
    copiando_reporte_al_clipboard,
    guardando_excel,
)

def connect_to_sga():
    try:
        logging.info("Verificando si la aplicación SGA está abierta...")
        app = Application(backend="uia").connect(title_re=".*SGA -")
        navegacion_window = app.window(title_re=".*SGA -")
        navegacion_window.set_focus()
        sleep(1)
        navegacion_window.maximize()
        sleep(1)
        logging.info("Conexión exitosa con la aplicación SGA.")
        return navegacion_window
    except Exception as e:
        logging.error(f"No se pudo conectar a la aplicación SGA: {e}")
        raise Exception("La aplicación SGA no está abierta o no está logueada. Por favor, verifica e inténtalo de nuevo.")

def connect_to_operaciones_Window():
    try:
        logging.info("Conectando a la ventana operaciones")
        operaciones_window = Desktop(backend="uia").window(title_re="SGA Operaciones.*")
        logging.info("Ventana principal de SGA Operaciones identificada.")
        return operaciones_window
    except Exception as e:
        logging.error(f"Error al identificar la ventana principal de SGA Operaciones: {e}")
        raise

class SGAService:
    def generate_dynamic_report(self):
        try:
            load_dotenv()
            excel_path = os.getenv('EXCEL_PATH')
            if not excel_path:
                raise EnvironmentError("Missing EXCEL_PATH in .env")

            os.makedirs('media', exist_ok=True)

            navegacion_window = connect_to_sga()
            navegar_sistema_tecnico(navegacion_window)
            seleccionar_opcion_sga(navegacion_window, "SGA Operaciones")
            sleep(10)

            operacion_window = connect_to_operaciones_Window()
            logging.info("Realizando operaciones en SGA Operaciones...")
            seleccionar_control_de_tareas(operacion_window)
            seleccionar_atcorp(operacion_window)
            abrir_reporte_dinamico(operacion_window)
            seleccionar_275_data_previa(operacion_window)
            seleccionar_fecha_secuencia(operacion_window)
            seleccionar_clipboard()
            numero_tickets = select_column_codiIncidencia()
            cerrar_reporte_Dinamico(operacion_window)
            seleccionar_atcorp(operacion_window)
            abrir_reporte_dinamico(operacion_window)
            seleccionar_276_averias(operacion_window)
            seleccionar_checkbox_nroincidencias(operacion_window)
            click_button_3puntos(operacion_window)
            seleccion_multiple_listado(numero_tickets)
            copiando_reporte_al_clipboard()
            guardando_excel()

            return {"status": "success", "file_path": excel_path}
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
