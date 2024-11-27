from fastapi import HTTPException
from time import sleep
from pywinauto import Application, Desktop
import os
import logging
from dotenv import load_dotenv
from datetime import datetime

from app.modules.sga.scripts.sga_navigation import navegar_sistema_tecnico, seleccionar_opcion_sga
from app.modules.sga.scripts.sga_operations import (
    seleccionar_control_de_tareas,
    seleccionar_atcorp,
    abrir_reporte_dinamico,
    seleccionar_275_data_previa,
    seleccionar_fecha_secuencia,
    seleccionar_fecha_secuencia_v2,
    seleccionar_fecha_secuencia_v3,
    seleccionar_clipboard,  
    select_column_codiIncidencia,
    seleccionar_276_averias,
    seleccionar_checkbox_nroincidencias,
    cerrar_reporte_Dinamico,
    click_button_3puntos,
    seleccion_multiple_listado,
    copiando_reporte_al_clipboard,
    guardando_excel,
    send_excel_to_api,
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
    async def generate_dynamic_report(self,fecha_secuencia_inicio,fecha_secuencia_fin) :
        try:
            load_dotenv()
            excel_path = os.getenv('EXCEL_PATH')
            if not excel_path:
                 raise EnvironmentError("Falta la variable de entorno EXCEL_PATH. Verifica el archivo .env.")

            if not os.path.exists('media/sga'):
                os.makedirs('media/sga')

            # os.makedirs('media', exist_ok=True)

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

     
            fecha_secuencia_inicio = datetime(2024, 11, 20)
            fecha_secuencia_fin = datetime(2024, 11, 27)
            fecha_secuencia_inicio_str = fecha_secuencia_inicio.strftime('%d/%m/%Y')
            fecha_secuencia_fin_str = fecha_secuencia_fin.strftime('%d/%m/%Y')

            seleccionar_fecha_secuencia_v2(operacion_window,fecha_secuencia_inicio_str, fecha_secuencia_fin_str)
            #seleccionar_fecha_secuencia_v3(operacion_window,"25/11/2024", "27/11/2024")
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
            path_excel = guardando_excel()
            if await send_excel_to_api(path_excel):
                return {
                    "status": "success",
                    "message":"Reporte enviado exitosamente"
                }
            else:
                raise HTTPException(
                    status_code=500,
                    detail="Error al enviar el archivo Excel a la API Django "
                )
            
            
        except Exception as e:
           error_message = f" Error al GENERAR Y ENVIAR EL REPORTE A LA API DJANGO: {str(e)}"
           logging.error(error_message)

           raise HTTPException(
                status_code=500,
                 detail=error_message
           )
