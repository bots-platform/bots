import os
import pandas as pd  # type: ignore
from fastapi import HTTPException
from pathlib import Path
from datetime import datetime, timedelta
from pywinauto import Application, Desktop
from time import sleep
from pywinauto.keyboard import send_keys
import pyperclip # type: ignore
from io import StringIO
import aiohttp
from aiohttp import ClientConnectorError
from urllib.parse import urlparse
from config import URL_DJANGO, EXCEL_FILENAME, EXCEL_CONTENT_TYPE, AUTH_USERNAME, AUTH_PASSWORD
from utils.logger_config import get_sga_logger
 
logger = get_sga_logger()



def seleccionar_control_de_tareas(main_window):
    try:
        logger.info("Intentando seleccionar 'Control de Tareas'.")
        main_window.set_focus()
        send_keys("%T") 
        sleep(1)
        send_keys("{DOWN 5}") 
        sleep(1)
        send_keys("{RIGHT}")
        send_keys("{ENTER}")
        logger.info("'Control de Tareas' seleccionado correctamente.")
    except Exception as e:
        logger.error(f"Error al seleccionar 'Control de Tareas': {e}")
        raise

def seleccionar_atcorp(main_window):
    try:
        logger.info("Intentando seleccionar 'ATCORP'.")
        atcorp = main_window.child_window(title="ATCORP", control_type="TreeItem")
        atcorp.click_input()
        logger.info("'ATCORP' seleccionado correctamente.")       
    except Exception as e:
        logger.error(f"Error al seleccionar 'ATCORP': {e}")
        raise

def abrir_reporte_dinamico(main_window):
    try:
        logger.info("Intentando abrir 'Reporte Dinámico'.")
        main_window.set_focus()
        send_keys("%F")
        sleep(1)
        send_keys("{DOWN 17}")
        sleep(1)
        send_keys("{ENTER}")
        sleep(2)
        logger.info("'Reporte Dinámico' abierto correctamente.")
    except Exception as e:
        logger.error(f"Error al abrir 'Reporte Dinámico': {e}")
        raise

def seleccionar_275_data_previa(main_window):
    try:
        logger.info("Intentando seleccionar '275 TABLERO DATA PREVIA'.")
        lista_reportes = main_window.child_window(title="Lista de Reportes por Area", control_type="Window")
        sleep(1)
        opciones_venta_panel = lista_reportes.child_window(title="Opciones de Venta", auto_id="1000", control_type="Pane")
        data_previa = opciones_venta_panel.child_window(title="compute_1", control_type="Text", found_index=18)
        data_previa.double_click_input()
        sleep(2)
        logger.info("'275 TTABLERO DATA PREVIA' seleccionado correctamente.")
    except Exception as e:
        logger.error(f"Error al seleccionar '275 TABLERO DATA PREVIA': {e}")
        raise

def seleccionar_fecha_secuencia(main_window, fecha_inicio=None, fecha_fin=None):
    try:
        logger.info("Intentando establecer rango de fecha de secuencia")
        logger.info(f"Estableciendo fecha de secuencia inicio: {fecha_inicio}")
        send_keys('{TAB}')
        sleep(1)
        pyperclip.copy(fecha_inicio)
        sleep(1)
        main_window.type_keys("^v")

        logger.info(f"Estableciendo fecha de secuencia fin: {fecha_fin}")
        send_keys('{TAB}')
        sleep(1)
        pyperclip.copy(fecha_fin)
        sleep(1)
        main_window.type_keys("^v")
        send_keys('{TAB 2}') 

        try:                                                                                                                                                                                                                                                                                                                                                                                
            logger.info("Intentando seleccionar el checkbox 'fecha_secuencia'")
            checkbox_fecha_secuencia = (
                main_window.child_window(title="fecha_secuencia", control_type="CheckBox")
                .wait('exists ready', timeout=3)
            )
            checkbox_fecha_secuencia.click()

            sleep(3)
            send_keys('{ENTER}')
            logger.info("Checkbox seleccionado exitosamente")
        except TimeoutError:
            logger.error("El CheckBox no estuvo listo a tiempo.")
            raise
        
        logger.info("Fechas de secuencia establecidas correctamente.")
    except Exception as e:
        logger.error(f"Error al establecer rango de fecha de secuencia: {e}")
        raise

def seleccionar_clipboard():
    try:
        sleep(1)
        logger.info("Copiando datos al clipboard")
        send_keys("%A")
        sleep(1)
        send_keys('{DOWN 4}')
        sleep(1)
        send_keys('{RIGHT}')
        send_keys('{DOWN 2}')
        send_keys('{ENTER}')
        logger.info("Tickets copiados correctamente")
    except Exception as e:
        logger.info(f"Error al copiar del 'clipboard': {e}")
        raise

def select_column_codiIncidencia():
    try:
        logger.info("Seleccionando la columna codigo de incidencias")
        sleep(2)      
        df = pd.read_clipboard(sep='\t')  
        sleep(1)
        codticket_data = df['codincidence'].iloc[0:].drop_duplicates()
        nro_tickets = len(codticket_data)
        sleep(1)
        result = '\n'.join(codticket_data.astype(str))
        pyperclip.copy(result)
        logger.info("Columna codigo de incidencias seleccionado correctamente")
        return nro_tickets
    except Exception as e:
        logger.info(f"Error al seleccionar la columna codigo de incidencias: {e}")
        raise
    
def cerrar_reporte_Dinamico(main_window):
    try:
        logger.info("Cerrando Reporte dinamico")
        reporte_dinamico = main_window.child_window(title="Reporte Dinamico", auto_id="202", control_type="Window")
        cerrar= reporte_dinamico.child_window(title="Cerrar", control_type="Button")
        cerrar.click_input()
        logger.info("Reporte dinamico cerrado correctamente")
    except Exception as e:
        logger.info(f"Error al cerrar 'Reporte dinamico': {e} ")
        raise

def seleccionar_276_averias(main_window):
   try:
       logger.info("Intentando seleccionar '276 AVERIAS'.")
       lista_reportes=main_window.child_window(title="Lista de Reportes por Area", control_type="Window")
       sleep(1)
       opciones_venta_panel = lista_reportes.child_window(title="Opciones de Venta", auto_id="1000", control_type="Pane")
       vertical_scrollbar = opciones_venta_panel.child_window(title="Vertical", auto_id="NonClientVerticalScrollBar", control_type="ScrollBar")
       scroll_bar = vertical_scrollbar.child_window(title="Av Pág", auto_id="DownPageButton", control_type="Button")
       for _ in range(1):  
           scroll_bar.click_input()
           sleep(0.5)  
       data_previa = opciones_venta_panel.child_window(title="compute_1", control_type="Text", found_index=6)
       data_previa.double_click_input()
       sleep(2)
       logger.info(" '276 AVERIAS' seleccionado correctamente ")
   except Exception as e:
       logger.error(f"Error al seleccionar '276 AVERIAS' : {e}")
       raise
   
def seleccionar_checkbox_nroincidencias(main_window):
    try:
        logger.info("Intentando seleccion check box")
        checkbox_fecha_secuencia = (
            main_window.child_window(title="nro_incidencia", control_type="CheckBox")
            .wait('exists ready', timeout=1)  
        )
        checkbox_fecha_secuencia.click()
        sleep(1)
        logger.info("checkBox selected successfully")
    except TimeoutError:
         logger.error("El CheckBox no estuvo listo a tiempo.")
         raise

def click_button_3puntos(main_window):  
      
    try:
        logger.info("Seleccionando el botón de '...'")
        filtros = main_window.window(title="Filtros", auto_id="1001", control_type="Pane")
        boton_tres_puntos = filtros.window(title="...", control_type="Button")
        sleep(2)
        boton_tres_puntos.click()
        sleep(0.5)
        logger.info("Botón '...' seleccionado correctamente.")
        return True
    except Exception as e:
        logger.error(f"Error al seleccionar 'button ...': {e}")
        

def seleccion_multiple_listado(numero_tickets):
    try:
        logger.info("Seleccionando multiple listado")
        send_keys('{TAB 2}')
        send_keys('{ENTER}')
        send_keys('+{TAB 2}')
        send_keys('{ENTER}')
        send_keys('{TAB}')
        send_keys('{ENTER}')
        for x in range(numero_tickets+1):
            send_keys('{TAB}')
        send_keys('{TAB}')
        send_keys('{ENTER}')
        send_keys('{ENTER}')  
    except Exception as e:
        logger.error(f"Error al seleccionar multiple listado")
        raise

def copiando_reporte_al_clipboard():
    try:
        logger.info("Copiando Reporte  al clipboard")
        sleep(25)
        send_keys("%A")
        sleep(1)
        send_keys('{DOWN 4}')
        sleep(1)
        send_keys('{RIGHT}')
        send_keys('{DOWN 2}')
        send_keys('{ENTER}')
        logger.info("Reporte copiados correctamente al clipboard")
    except Exception as e:
        logger.info(f"Error al copiar del 'clipboard': {e}")
        raise

def guardando_excel(fecha_procesada):

    try:
        logger.info("Guardando reporte del clipboard al excel")   
        base_dir = 'media'
        sga_dir = os.path.join(base_dir, 'sga')
        if not os.path.exists(sga_dir):
            os.makedirs(sga_dir)
            logger.info(f"Directorio '{sga_dir}' creado.")

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'{sga_dir}/reporte_{fecha_procesada}_{timestamp}.xlsx'

        df = pd.read_clipboard(sep='\t')
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='reporte')

        logger.info("Reporte guardado en excel correctamente")
        return output_file
      
    except Exception as e:
        logger.info(f"Error al guardar reporte del clipboard al excel: {e}")
        return {"status": "error", "message": str(e)}

async def send_excel_to_api(excel_path):

    try:
        logger.info("Tratando de enviar el excel a la API Django")

        parsed = urlparse(URL_DJANGO)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"La URL '{URL_DJANGO} no esta correctamente configurada.")
    
        if not Path(excel_path).exists():
            logger.error(f" Archivo excel no encontrado: {excel_path}")
            return False

        mi_url = URL_DJANGO

        form_data = aiohttp.FormData()
        form_data.add_field( 
                            'sga_csv',
                            open(excel_path, 'rb'), 
                            filename=EXCEL_FILENAME,
                            content_type=EXCEL_CONTENT_TYPE
                            )

        form_data.add_field('sga_fecha',
                           datetime.now().strftime('%Y-%m-%d'))
            
        async with aiohttp.ClientSession() as session:
            auth = aiohttp.BasicAuth(AUTH_USERNAME, AUTH_PASSWORD)
            async with session.post(
                url=mi_url,
                data=form_data,
                auth=auth
            ) as response:
                if response.status in [200, 202]:
                    logger.info("Excel enviado exitosamente a la Django")
                    return {
                        "status":"success",
                        "message": "Archivo enviado correctamente"
                    }
                else:
                    error_message = await response.text()
                    logger.error(f"Error al enviar Excel a Django: {response.status},{error_message}")
                    return {
                        "status":"error",
                        "message": f"Error al enviar Excel a Django : {error_message}"
                    }
                
    except ValueError as e:
        raise HTTPException(status_code=500, detail=f"Error en la configuracion del la URL: {str(e)} ")
                
    except ClientConnectorError as e:
        logger.exception(f"Error de conexión con el servidor: {str(e)}")
        return {
            "status":"error",
            "message": f"Servicio no disponible. El servidor esta caido o inalcanzable: {str(e)}."
        }
            
    except Exception as e:
        logger.exception(f"Error general al enviar excel a la api Django: {e}")
        return {
            "status": "error",
            "message": str(e)
        }
    
