import os
import logging
import pandas as pd  # type: ignore
from fastapi import HTTPException
import requests
from pathlib import Path
from datetime import datetime
from pywinauto import Application, Desktop
from time import sleep
from pywinauto.keyboard import send_keys
import pyperclip # type: ignore
from io import StringIO
import aiohttp


if not os.path.exists('app/modules/sga/logs'):
    os.makedirs('app/modules/sga/logs')

logging.basicConfig(level=logging.INFO, filename="app/modules/sga/logs/sga_operations.log", 
                    format="%(asctime)s - %(levelname)s - %(message)s")

def seleccionar_control_de_tareas(main_window):
    try:
        logging.info("Intentando seleccionar 'Control de Tareas'.")
        main_window.set_focus()
        send_keys("%T") 
        sleep(1)
        send_keys("{DOWN 5}") 
        sleep(1)
        send_keys("{RIGHT}")
        send_keys("{ENTER}")
        logging.info("'Control de Tareas' seleccionado correctamente.")
    except Exception as e:
        logging.error(f"Error al seleccionar 'Control de Tareas': {e}")
        raise

def seleccionar_atcorp(main_window):
    try:
        logging.info("Intentando seleccionar 'ATCORP'.")
        atcorp = main_window.child_window(title="ATCORP", control_type="TreeItem")
        atcorp.click_input()
        logging.info("'ATCORP' seleccionado correctamente.")       
    except Exception as e:
        logging.error(f"Error al seleccionar 'ATCORP': {e}")
        raise

def abrir_reporte_dinamico(main_window):
    try:
        logging.info("Intentando abrir 'Reporte Dinámico'.")
        main_window.set_focus()
        send_keys("%F")
        sleep(1)
        send_keys("{DOWN 17}")
        sleep(1)
        send_keys("{ENTER}")
        sleep(2)
        logging.info("'Reporte Dinámico' abierto correctamente.")
    except Exception as e:
        logging.error(f"Error al abrir 'Reporte Dinámico': {e}")
        raise

def seleccionar_275_data_previa(main_window):
    try:
        logging.info("Intentando seleccionar '275 TABLERO DATA PREVIA'.")
        lista_reportes = main_window.child_window(title="Lista de Reportes por Area", control_type="Window")
        sleep(1)
        opciones_venta_panel = lista_reportes.child_window(title="Opciones de Venta", auto_id="1000", control_type="Pane")
        data_previa = opciones_venta_panel.child_window(title="compute_1", control_type="Text", found_index=18)
        data_previa.double_click_input()
        sleep(2)
        logging.info("'275 TTABLERO DATA PREVIA' seleccionado correctamente.")
    except Exception as e:
        logging.error(f"Error al seleccionar '275 TABLERO DATA PREVIA': {e}")
        raise

def seleccionar_fecha_secuencia(main_window):
     try:
        logging.info("Intentando establecer rango de fecha de secuencia")  
        try:
            logging.info("Intentando seleccion check box")
            checkbox_fecha_secuencia = (
                #main_window.child_window(best_match="fecha_secuenciaCheckBox", control_type="CheckBox")
                main_window.child_window(title="fecha_secuencia", control_type="CheckBox")
                .wait('exists ready', timeout=3)  
            )
            checkbox_fecha_secuencia.click()
            sleep(1)
            send_keys('{ENTER}')
            logging.info("checkBox selected successfully")
            #main_window.print_control_identifiers()
        except TimeoutError:
            logging.error("El CheckBox no estuvo listo a tiempo.")
     except Exception as e:
         logging.error(f"Error al establecer rango de fecha de secuencia: {e}")
         raise
     
def seleccionar_clipboard():
    try:
        sleep(1)
        logging.info("Copiando datos al clipboard")
        send_keys("%A")
        sleep(1)
        send_keys('{DOWN 4}')
        sleep(1)
        send_keys('{RIGHT}')
        send_keys('{DOWN 2}')
        send_keys('{ENTER}')
        logging.info("Tickets copiados correctamente")
    except Exception as e:
        logging.info(f"Error al copiar del 'clipboard': {e}")
        raise

def select_column_codiIncidencia():
    try:
        logging.info("Seleccionando la columna codigo de incidencias")
        sleep(2)      
        df = pd.read_clipboard(sep='\t')  
        sleep(1)
        codticket_data = df['codincidence'].iloc[0:].drop_duplicates()
        nro_tickets = len(codticket_data)
        sleep(1)
        result = '\n'.join(codticket_data.astype(str))
        pyperclip.copy(result)
        logging.info("Columna codigo de incidencias seleccionado correctamente")
        return nro_tickets
    except Exception as e:
        logging.info(f"Error al seleccionar la columna codigo de incidencias: {e}")
        return False
    
def cerrar_reporte_Dinamico(main_window):
    try:
        logging.info("Cerrando Reporte dinamico")
        reporte_dinamico = main_window.child_window(title="Reporte Dinamico", auto_id="202", control_type="Window")
        cerrar= reporte_dinamico.child_window(title="Cerrar", control_type="Button")
        cerrar.click_input()
        logging.info("Reporte dinamico cerrado correctamente")
    except Exception as e:
        logging.info(f"Error al cerrar 'Reporte dinamico': {e} ")
        raise

def seleccionar_276_averias(main_window):
   try:
       logging.info("Intentando seleccionar '276 AVERIAS'.")
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
       logging.info(" '276 AVERIAS' seleccionado correctamente ")
   except Exception as e:
       logging.error(f"Error al seleccionar '276 AVERIAS' : {e}")
       raise
   
def seleccionar_checkbox_nroincidencias(main_window):
    try:
        logging.info("Intentando seleccion check box")
        checkbox_fecha_secuencia = (
            main_window.child_window(title="nro_incidencia", control_type="CheckBox")
            .wait('exists ready', timeout=1)  
        )
        checkbox_fecha_secuencia.click()
        sleep(1)
        logging.info("checkBox selected successfully")
    except TimeoutError:
         logging.error("El CheckBox no estuvo listo a tiempo.")

def click_button_3puntos(main_window):  
      
    try:
        logging.info("Seleccionando el botón de '...'")
        filtros = main_window.window(title="Filtros", auto_id="1001", control_type="Pane")
        boton_tres_puntos = filtros.window(title="...", control_type="Button")
        sleep(2)
        boton_tres_puntos.click()
        sleep(0.5)
        logging.info("Botón '...' seleccionado correctamente.")
        return True
    except Exception as e:
        logging.error(f"Error al seleccionar 'button ...': {e}")
        return False

def seleccion_multiple_listado(numero_tickets):
    try:
        logging.info("Seleccionando multiple listado")
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
        logging.error(f"Error al seleccionar multiple listado")

def copiando_reporte_al_clipboard():
    try:
        logging.info("Copiando Reporte  al clipboard")
        sleep(15)
        send_keys("%A")
        sleep(1)
        send_keys('{DOWN 4}')
        sleep(1)
        send_keys('{RIGHT}')
        send_keys('{DOWN 2}')
        send_keys('{ENTER}')
        logging.info("Reporte copiados correctamente al clipboard")
    except Exception as e:
        logging.info(f"Error al copiar del 'clipboard': {e}")
        raise

def guardando_excel():
    try:
        logging.info("Guardando reporte del clipboard al excel")
        sleep(2)      
        df = pd.read_clipboard(sep='\t')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'app/modules/sga/media/reporte_{timestamp}.xlsx'
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='reporte')
        logging.info("Reporte guardado en excel correctamente")
      
        return output_file

      
    except Exception as e:
        logging.info(f"Error al guardar reporte del clipboard al excel: {e}")
        return False

async def send_excel_to_api(excel_path):
    try:
        if not Path(excel_path).exists():
            logging.error(f" Archivo excel no encontrado: {excel_path}")
            return False
        
        
        df = pd.read_excel(excel_path)
        csv_path = excel_path.replace('.xlsx', '.csv')
        df.to_csv(csv_path, index=False)
            
        mi_url = "http://10.200.90.248:9000/upload-sga-tickets/"

        form_data = aiohttp.FormData()
        form_data.add_field( 
                            'sga_csv',
                            open(excel_path, 'rb'),  # Abrimos el archivo como un objeto de archivo directamente
                            filename='sga_report.xlsx',
                            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                            )

    
        form_data.add_field('sga_fecha',
                           datetime.now().strftime('%Y-%m-%d'))
            
        async with aiohttp.ClientSession() as session:
            auth = aiohttp.BasicAuth('admin', 'admin')
            async with session.post(
                url=mi_url,
                data=form_data,
                auth=auth
            ) as response:
                # print(response.text)
                if response.status in [200, 202]:
                    logging.info("Excel enviado exitosamente")
                    return {
                        "status":"success",
                        "message": "Archivo enviado correctamente"
                    }
                else:
                    logging.error(f"Error al enviar Excel: {response.status}")
                    raise HTTPException(
                        status_code=response.status,
                        detail=f"Error al enviar Excel: {await response.text()}"
                    )
            
    except Exception as e:
        logging.exception(f"Error en envío de Excel: {e}")
        raise HTTPException(status_code=500, detail=str(e))