import os
import pandas as pd  # type: ignore
from datetime import datetime
import time
import re
from time import sleep
from pywinauto.keyboard import send_keys
import pyperclip 
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
        sleep(1)
        logger.info("'Control de Tareas' seleccionado correctamente.")
    except Exception as e:
        logger.error(f"Error al seleccionar 'Control de Tareas': {e}")
        raise

def seleccionar_atcorp(main_window):
    try:
        logger.info("Intentando seleccionar 'ATCORP'.")
        atcorp = main_window.child_window(title="ATCORP", control_type="TreeItem")
        sleep(7)
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

def seleccionar_data_previa(main_window, indice_reporte_tickets):
    try:
        logger.info("Intentando seleccionar '275 TABLERO DATA PREVIA'.")
        lista_reportes = main_window.child_window(title="Lista de Reportes por Area", control_type="Window")
        sleep(1)
        opciones_venta_panel = lista_reportes.child_window(title="Opciones de Venta", auto_id="1000", control_type="Pane")
        if indice_reporte_tickets == 13 :
            vertical_scrollbar = opciones_venta_panel.child_window(title="Vertical", auto_id="NonClientVerticalScrollBar", control_type="ScrollBar")
            scroll_bar = vertical_scrollbar.child_window(title="Av Pág", auto_id="DownPageButton", control_type="Button")
            for _ in range(1):  
               scroll_bar.click_input()
               sleep(0.5)  
        data_previa = opciones_venta_panel.child_window(title="compute_1", control_type="Text", found_index=indice_reporte_tickets)
        sleep(2)
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
        sleep(1)
        logger.info(f"Estableciendo fecha de secuencia fin: {fecha_fin}")
        send_keys('{TAB}')
        sleep(1)
        pyperclip.copy(fecha_fin)
        sleep(1)
        main_window.type_keys("^v")
        sleep(1)
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
        sleep(1)
        send_keys('{DOWN 2}')
        sleep(1)
        send_keys('{ENTER}')
        sleep(1)
        logger.info("Tickets copiados correctamente")
    except Exception as e:
        logger.info(f"Error al copiar del 'clipboard': {e}")
        raise

def cerrar_reporteDinamico_276():
    try:
        sleep(1)
        logger.info("Cerrando reporte dinamico 276")
        send_keys("%A")
        sleep(1)
        send_keys('{DOWN 5}')
        sleep(1)
        send_keys('{ENTER}')
        sleep(1)
        logger.info("cerrado correctamente")
    except Exception as e:
        logger.info(f"Error al cerrar repor dinamico 276: {e}")
        raise

def select_column_codiIncidencia():
    try:
        logger.info("Seleccionando la columna codigo de incidencias")
        sleep(2)      
        df = pd.read_clipboard(sep='\t', header=0)  
        sleep(1)
        codticket_data = df['codincidence'].astype(str).str.strip().drop_duplicates().reset_index(drop=True)
        nro_tickets = len(codticket_data)
        sleep(1)
        result = '\n'.join(codticket_data.astype(str))
        pyperclip.copy(result)
        sleep(1)
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

def seleccionar_averias_detalle(main_window, indice_reporte_detalle):
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
       data_previa = opciones_venta_panel.child_window(title="compute_1", control_type="Text", found_index=indice_reporte_detalle)
       sleep(2)
       data_previa.double_click_input()
       sleep(2)
       logger.info(" ' Detalle AVERIAS' seleccionado correctamente ")
   except Exception as e:
       logger.error(f"Error al seleccionar '276 AVERIAS' : {e}")
       raise
   
def seleccionar_checkbox_nroincidencias(main_window):
    try:
        logger.info("Intentando seleccion check box")
        checkbox_fecha_secuencia = (
            main_window.child_window(title="nro_incidencia", control_type="CheckBox")
            .wait('exists ready', timeout=3)  
        )
        checkbox_fecha_secuencia.click()
        sleep(2)
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
        sleep(1)
        logger.info("Botón '...' seleccionado correctamente.")
        return True
    except Exception as e:
        logger.error(f"Error al seleccionar 'button ...': {e}")
    
def seleccion_multiple_listado(numero_tickets):
    try:
        sleep(1)
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
        sleep(70)
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

def generando_reporte_sga(main_window, fecha_inicio, fecha_fin, indice_reporte_detalle):
    chunk_size = 990
    try:
        logger.info("Seleccionando la columna codigo de incidencias, partiendo en lotes de  990 tickets (limit sga consulta detalle) and generate final reporte")
        sleep(2)      
        df = pd.read_clipboard(sep='\t', header=0)  
        sleep(1)
        codticket_data_antes = df['codincidence']
        logger.info(f"total tickets antes de eliminar duplicates {len(codticket_data_antes)}")
        
        codticket_data = df['codincidence'].astype(str).str.strip().drop_duplicates().reset_index(drop=True)
        sleep(1)
        total_tickets = len(codticket_data)
        logger.info(f"Total unique tickets founded {total_tickets}")

        list_of_dfs = []

        for start in range(0, total_tickets, chunk_size):
            end = min(start + chunk_size, total_tickets)
            chunk_tickets = codticket_data.iloc[start:end]

            if chunk_tickets.empty:
                logger.warning(f"Skipping empty chunk from {start} to {end}")

    
            chunk_tickets_lenght = len(chunk_tickets)
            tickets_str = '\n'.join(chunk_tickets.astype(str))
            pyperclip.copy(tickets_str)
            logger.info(f"Tickets del indice {start} hasta {end -1} copiados al portapapeles")
        
            seleccionar_atcorp(main_window)
            abrir_reporte_dinamico(main_window)
            seleccionar_averias_detalle(main_window, indice_reporte_detalle)
            seleccionar_checkbox_nroincidencias(main_window)
            click_button_3puntos(main_window)
            seleccion_multiple_listado(chunk_tickets_lenght)
            copiando_reporte_al_clipboard()
            sleep(1)
            partial_df = pd.read_clipboard(sep='\t')
            sleep(1)
            list_of_dfs.append(partial_df)
            sleep(1)
            cerrar_reporteDinamico_276()
        
        final_df = pd.concat(list_of_dfs, ignore_index=True)

        if indice_reporte_detalle == 4: # General
            pattern_non_alnum = re.compile(r"[^a-zA-Z0-9 ]")
            final_df["remedyobs"] = final_df["remedyobs"].str.replace(pattern_non_alnum, '', regex=True)
        
        logger.info("Seleccionando la columna codigo de incidencias, partiendo en lotes de  990 tickets (limit sga consulta detalle) and generate final reporte procesada y consolidada correctamente.")
        
        path_excel_sga = guardando_a_excel(fecha_inicio, fecha_fin, final_df)
        
        max_wait_time = 20
        check_interval = 2

        elapsed_time = 0
        while not os.path.exists(path_excel_sga) and elapsed_time < max_wait_time:
            time.sleep(check_interval)
            elapsed_time += check_interval

        if os.path.exists(path_excel_sga):
            logger.info(f"Reporte SGA guardado correnctmente en {path_excel_sga} tardo {elapsed_time}")
        else:
            logger.error(f"El archivo no se genero en el tiempo maximo tiempo de {max_wait_time}")
            raise Exception("El archivo no se genero correctamente")
        
        return path_excel_sga
    
    except Exception as e:
        logger.info(f"Error Seleccionando la columna codigo de incidencias, partiendo en lotes de  990 tickets (limit sga consulta detalle) and generate  reporte: {e}")
        

def guardando_a_excel(fecha_inicio, fecha_fin, final_df_sga):

    try:
        logger.info("tratando de guardar")
        output_dir = 'media/sga/reporte_SGA/'
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        fecha_inicio_format = fecha_inicio.replace("/", "-")
        fecha_fin_format = fecha_fin.replace("/", "-")

        path_reporte_sga = os.path.join(output_dir, f'sga_reporte_{fecha_inicio_format}_{fecha_fin_format}_{timestamp}.xlsx')
        final_df_sga.to_excel(path_reporte_sga, index=False, engine='openpyxl')
        logger.info(f"Reporte sga guardado en: {path_reporte_sga}")

        return path_reporte_sga
    except Exception as e :
        logger.error(f"error al generar el archivo : {e}")
        raise



