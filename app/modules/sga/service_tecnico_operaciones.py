from fastapi import HTTPException
from time import sleep
from pywinauto import Application, Desktop
import os
import logging
from datetime import datetime, timedelta

from app.modules.sga.scripts.sga_navigation import navegar_sistema_tecnico, seleccionar_opcion_sga
from app.modules.sga.scripts.cumplimiento_sla.cumplimiento_sla_fill_columns import completar_columnas_faltantes_con_python
from app.modules.sga.scripts.sga_operations import (
    seleccionar_control_de_tareas,
    seleccionar_atcorp,
    abrir_reporte_dinamico,
    seleccionar_data_previa,
    seleccionar_fecha_secuencia,
    seleccionar_clipboard,  
    cerrar_reporte_Dinamico,
    generando_reporte_sga,
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
        logging.info("Intentando identificar la ventana operaciones")
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
    def generate_dynamic_report(self,fecha_secuencia_inicio,fecha_secuencia_fin, indice_tabla_reporte_data_previa, indice_tabla_reporte_detalle) :
        path_excel_sga_sla_report = None
        path_excel_sga_report = None
        try:
            
            navegacion_window = connect_to_sga()
            navegar_sistema_tecnico(navegacion_window)
            seleccionar_opcion_sga(navegacion_window, "SGA Operaciones")
            sleep(10)

            operacion_window = connect_to_operaciones_Window()
            logging.info("Realizando operaciones en SGA Operaciones...")
            seleccionar_control_de_tareas(operacion_window)
             
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            resultados = []
           
            try:
                
                fecha_inicio_str = fecha_secuencia_inicio.strftime('%d/%m/%Y')
                fecha_fin_str = fecha_secuencia_fin.strftime('%d/%m/%Y')

                logging.info(f"Procesando generar reporte dinamico para la fecha: {fecha_inicio_str} - {fecha_fin_str} -{timestamp}")
                
                seleccionar_atcorp(operacion_window)
                abrir_reporte_dinamico(operacion_window)
                seleccionar_data_previa(operacion_window, indice_tabla_reporte_data_previa)
                seleccionar_fecha_secuencia(operacion_window,fecha_inicio_str, fecha_fin_str)
                seleccionar_clipboard()
                cerrar_reporte_Dinamico(operacion_window)
                
                path_excel_sga_report = generando_reporte_sga(operacion_window, fecha_inicio_str, fecha_fin_str, indice_tabla_reporte_detalle)

                #path_excel_sga_sla_report = completar_columnas_faltantes_con_python(path_excel_sga_report, fecha_inicio_str, fecha_fin_str)

            except Exception as e:
                logging.error(f"Error al procesar la fecha  {fecha_inicio_str} - {fecha_fin_str} -{timestamp}: {e}")
                resultados.append(
                    {
                    "fecha":  {fecha_inicio_str} - {fecha_fin_str} -{timestamp},  
                    "status": "error",
                    "message": f"Error interno para la fecha: {str(e)}"
                    }
                )
 
            close_operaciones_window(operacion_window)

            if indice_tabla_reporte_detalle == 4: 
                return path_excel_sga_sla_report
            
            if indice_tabla_reporte_detalle == 15:
                return path_excel_sga_report
          
        except Exception as e:
           error_message = f" Error general al generar el reporte dinamico: {str(e)}"
           logging.error(error_message)
           raise HTTPException(
                status_code=500,
                detail=error_message
           )








