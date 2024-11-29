from fastapi import HTTPException
from time import sleep
from pywinauto import Application, Desktop
import os
import logging
from datetime import datetime, timedelta

from app.modules.sga.scripts.sga_navigation import navegar_sistema_tecnico, seleccionar_opcion_sga
from app.modules.sga.scripts.sga_operations import (
    seleccionar_control_de_tareas,
    seleccionar_atcorp,
    abrir_reporte_dinamico,
    seleccionar_275_data_previa,
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

def connect_to_operaciones_Window():
    try:
        logging.info("Conectando a la ventana operaciones")
        operaciones_window = Desktop(backend="uia").window(title_re="SGA Operaciones.*")
        logging.info("Ventana principal de SGA Operaciones identificada.")
        return operaciones_window
    except Exception as e:
        logging.error(f"Error al identificar la ventana principal de SGA Operaciones: {e}")
        raise

def close_operaciones_window(operacion_window):
    try:
        logging.info("Intentando cerrar la ventana de operaciones...")

        if operacion_window.exists() and operacion_window.is_visible():
            operacion_window.close()
            logging.info("Ventana de operaciones cerrada exitosamente.")
        else:
            logging.warning("La ventana de operaciones no está visible o no existe.")
    except Exception as e:
        logging.error(f"Error al intentar cerrar la ventana de operaciones: {e}")
        raise

class SGAService:
    async def generate_dynamic_report(self,fecha_secuencia_inicio,fecha_secuencia_fin) :
        try:

            navegacion_window = connect_to_sga()
            navegar_sistema_tecnico(navegacion_window)
            seleccionar_opcion_sga(navegacion_window, "SGA Operaciones")
            sleep(10)

            operacion_window = connect_to_operaciones_Window()
            logging.info("Realizando operaciones en SGA Operaciones...")
            seleccionar_control_de_tareas(operacion_window)
            

            if isinstance(fecha_secuencia_inicio, str):
                fecha_secuencia_inicio = datetime.strptime(fecha_secuencia_inicio, "%Y-%m-%d")
            if isinstance(fecha_secuencia_fin, str):
                fecha_secuencia_fin = datetime.strptime(fecha_secuencia_fin, "%Y-%m-%d")

            
            fecha_actual = fecha_secuencia_inicio
            resultados = []
            while fecha_actual <= fecha_secuencia_fin:
                try:
                  
                    fecha_actual_str = fecha_actual.strftime('%d/%m/%Y')
            
                    logging.info(f"Procesando fecha: {fecha_actual_str}")
                   
                    seleccionar_atcorp(operacion_window)
                    abrir_reporte_dinamico(operacion_window)
                    seleccionar_275_data_previa(operacion_window)
                    seleccionar_fecha_secuencia_v3(operacion_window, fecha_actual_str, fecha_actual_str)
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
                    cerrar_reporte_Dinamico(operacion_window)
                    path_excel = guardando_excel()

                    if await send_excel_to_api(path_excel):
                        logging.info(f"Reporte enviado exitosamente para la fecha: {fecha_actual_str}")
                        resultados.append({
                            "fecha":fecha_actual_str,
                            "status":"success",
                            "message":"Reporte enviado exitosamente"
                        })      
                    else:
                        logging.error(f"Error al enviar el archivo Excel para la fecha: {fecha_actual_str}")
                        resultados.append(
                            {
                                "fecha":fecha_actual_str,
                                "status":"error",
                                "message":"Error al enviar el archivo"
                            }
                        )
                except Exception as e:
                    logging.error(f"Error al procesar la fecha {fecha_actual_str}: {e}")
                    resultados.append(
                        {"fecha": fecha_actual_str, "status": "error", "message": str(e)}
                    )

                logging.info(f"fecha antes de sumar: {fecha_actual}")
                fecha_actual += timedelta(days=1)
                logging.info(f"fecha despues de sumar: {fecha_actual}")
            close_operaciones_window(operacion_window)
            return{
                "status":"finalizado",
                "Resultados": resultados
            }
        except Exception as e:
           error_message = f" Error al enviar reporte: {str(e)}"
           logging.error(error_message)
           raise HTTPException(
                status_code=500,
                 detail=error_message
           )
