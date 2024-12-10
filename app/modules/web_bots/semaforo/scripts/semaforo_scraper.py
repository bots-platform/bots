from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

from app.modules.web_bots.browser.setup import setup_chrome_driver
from  ...utils.input_utils  import random_delay
import time
from datetime import datetime, timedelta
from config import URL_SEMAFORO
from utils.logger_config import get_semaforo_logger

from bs4 import BeautifulSoup
import pandas as pd
import os
logger = get_semaforo_logger()




def login_to_semaforo(driver, user, password):

    try:
        logger.info(f"Intentando login SEMAFORO")
        driver.get(URL_SEMAFORO)
        random_delay(3, 5)  
        
        user_input = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, 'username'))
            )
        user_input.clear()
        for char in user:
            user_input.send_keys(char)
            time.sleep(0.1)

        password_input = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.ID, 'password'))
            )
        password_input.clear()
        for char in password:
            password_input.send_keys(char)
            time.sleep(0.1) 

        loggin_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.btn.btn-primary.btn-block'))
        )
        loggin_button.click()

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "navbar-static-top"))
        )

        logger.info(f"login exitoso")
        return True
    
    except Exception as e:
        logger.error(f"Falló login {str(e)}")
        return False
    
def navegar_reportes_asistencia(driver):
    logger.info('Seleccionando opcion reportes Activaidad Agente')
    try:
        
        time.sleep(1)
        driver.get("http://10.200.81.218:3000/atcorp/signInAnalystsReport")

        logger.info('opcion Reporte de asistencia seleccionada exitosamente')

    except Exception as e:
        error_message = f'fallo al ingresar a reporte de asistencia: { str (e)}'
        logger.error(error_message)
        raise Exception(error_message)
    
def set_fechas_filtro(driver, fecha_desde, fecha_hasta):
    try:
        logger.info(f'Estableciendo fechas de filtro: desde {fecha_desde} hasta {fecha_hasta}')
        
      
        fecha_desde_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//label[text()='Fecha(Desde)']/following-sibling::input"))
        )
        fecha_desde_input.clear()
        fecha_desde_input.send_keys(fecha_desde.strftime("%d/%m/%Y"))
        
        time.sleep(1)
       
        fecha_hasta_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//label[text()='Fecha(Hasta)']/following-sibling::input"))
        )
        fecha_hasta_input.clear()
        fecha_hasta_input.send_keys(fecha_hasta.strftime("%d/%m/%Y"))
        
        logger.info('Fechas establecidas exitosamente')
        return True
        
    except Exception as e:
        error_message = f'Error al establecer fechas de filtro: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)

def click_filtrar(driver):
    try:
        logger.info('Haciendo clic en botón Filtrar')
        time.sleep(1)
        filtrar_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[text()='Filtrar']"))
        )
        
        filtrar_button.click()
        logger.info('Clic en Filtrar exitoso')
        return True
        
    except Exception as e:
        error_message = f'Error al hacer clic en Filtrar: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)

def click_descargar_excel(driver):
    try:
        logger.info('Intentando descargar formato excel')
        
    
        download_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[text()='Descargar formato excel']"))
        )
        
        download_button.click()

        ok_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "swal2-confirm"))
        )
        ok_button.click()

       
        time.sleep(2)

        logger.info(f'Excel descargado exitosamente en ')
        return True
        
    except Exception as e:
        error_message = f'Error al intentar descargar excel: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)
    
def extract_and_save_table_semaforo(driver):
    logger.info('Intentando extraer y guardar datos de la tabla de semáforo')
    try:
        all_rows = []
        current_page = 1
        
        # 1. Encontrar el elemento que contiene el total
        total_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.d-inline-block.pr-2 span"))
        )
        
        # 2. Obtener el número total limpio
        total_text = total_element.text
        total_records = int(''.join(filter(str.isdigit, total_text)))
        logger.info(f'Total de registros a procesar: {total_records}')
        
        while True:
            # 3. Esperar y obtener la tabla
            table = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-bordered"))
            )
            time.sleep(2)
            
            # 4. Obtener HTML y parsearlo
            html_content = table.get_attribute('outerHTML')
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 5. Extraer filas
            tbody = soup.find('tbody')
            if tbody:
                for tr in tbody.find_all('tr'):
                    cells = tr.find_all('td')
                    if cells:
                        row_data = [cell.text.strip() for cell in cells]
                        all_rows.append(row_data)
            
            logger.info(f'Procesada página {current_page}')
            
            # 6. Navegación de páginas
            try:
                # Buscar el botón siguiente usando la clase correcta
                next_button = driver.find_element(By.CSS_SELECTOR, "a.page-link[aria-label='Next']")
                if 'disabled' in next_button.get_attribute('class'):
                    break
                next_button.click()
                current_page += 1
                time.sleep(2)
            except Exception as e:
                logger.info('No hay más páginas para procesar')
                break
        
        # 7. Crear y guardar DataFrame
        headers = ['#', 'CELDA', 'PUESTO', 'ANALISTA', 'HORARIO', 'MODALIDAD', 'FECHA', 'HORA DE INGRESO']
        df = pd.DataFrame(all_rows, columns=headers)
        
        os.makedirs('media/semaforo', exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'media/semaforo/reporte_semaforo_{timestamp}.xlsx'
        df.to_excel(filename, index=False)
        
        logger.info(f'Datos guardados exitosamente en {filename}. Total de registros procesados: {len(df)}')
        return filename
        
    except Exception as e:
        error_message = f'Error al extraer/guardar datos de la tabla de semáforo: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)
        
def scrape_semaforo_page(driver, user, password, fecha_desde, fecha_hasta):
 
    login_to_semaforo(driver, user, password)
    navegar_reportes_asistencia(driver)
    set_fechas_filtro(driver, fecha_desde, fecha_hasta)
    click_filtrar(driver)
    extract_and_save_table_semaforo(driver)
























    

# def extract_and_save_table_semaforo(driver):
#     logger.info('Intentando extraer y guardar datos de la tabla de semáforo')
#     try:
        
#         table = WebDriverWait(driver, 10).until(
#             EC.presence_of_element_located((By.CLASS_NAME, "table-bordered"))
#         )
        
        
#         html_content = table.get_attribute('outerHTML')
        
       
#         soup = BeautifulSoup(html_content, 'html.parser')
        
      
#         headers = ['#', 'CELDA', 'PUESTO', 'ANALISTA', 'HORARIO', 'MODALIDAD', 'FECHA', 'HORA DE INGRESO']
        
      
#         rows = []
#         for tr in soup.find('tbody').find_all('tr'):
#             cells = tr.find_all('td')
#             if cells:  
#                 row = []
#                 for td in cells:
#                     row.append(td.text.strip())
#                 rows.append(row)
        
        
#         df = pd.DataFrame(rows, columns=headers)
        
        
#         os.makedirs('media/semaforo', exist_ok=True)
        
#         timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
#         filename = f'media/semaforo/reporte_semaforo_{timestamp}.xlsx'
        
        
#         df.to_excel(filename, index=False)
        
#         logger.info(f'Datos de semáforo guardados exitosamente en {filename}')
#         return filename
        
#     except Exception as e:
#         error_message = f'Error al extraer/guardar datos de la tabla de semáforo: {str(e)}'
#         logger.error(error_message)
#         raise Exception(error_message)



