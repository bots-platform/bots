from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

# from app.modules.web_bots.browser.setup import setup_chrome_driver
from  ...utils.input_utils  import random_delay
import time
from datetime import datetime, timedelta
from config import URL_SEMAFORO
from utils.logger_config import get_semaforo_logger
from utils.waiting_download import wait_for_download

from bs4 import BeautifulSoup
import pandas as pd
import os
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
logger = get_semaforo_logger()


def handle_download_dialog(driver):
    try:
        # Esperar y hacer clic en el botón Keep
        keep_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[text()='Keep']"))
        )
        keep_button.click()
        
        logger.info("Descarga confirmada exitosamente")
        return True
        
    except Exception as e:
        # Si no funciona el clic, intentar con teclas
        try:
            ActionChains(driver)\
                .key_down(Keys.ALT).send_keys('k').key_up(Keys.ALT)\
                .perform()
            logger.info("Descarga confirmada usando atajos de teclado")
            return True
        except Exception as sub_e:
            logger.error(f"Error al manejar diálogo de descarga: {str(e)} - {str(sub_e)}")
            return False

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
        time.sleep(1)
        logger.info('Fechas establecidas exitosamente')
        return True
        
    except Exception as e:
        error_message = f'Error al establecer fechas de filtro: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)

def click_filtrar(driver):
    try:
        logger.info('Haciendo clic en botón Filtrar')
        time.sleep(2)
        filtrar_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[text()='Filtrar']"))
        )
        
        filtrar_button.click()
        time.sleep(2)
        logger.info('Clic en Filtrar exitoso')
        return True
        
    except Exception as e:
        error_message = f'Error al hacer clic en Filtrar: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)

def click_descargar_excel(driver,fecha_desde, fecha_hasta):
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

        download_path = os.path.abspath("media/semaforo/")

        downloaded_file = wait_for_download(download_path, timeout=60, polling_interval=1)

        if not downloaded_file:
            raise Exception("El archivo no se descargó dentro del tiempo de espera")

        logger.info(f"Archivo descargado encontrado: {downloaded_file}")

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        new_file_name = f"reporte_semaforo_{fecha_desde}_{fecha_hasta}_{timestamp}.xls"

        new_file_path = os.path.join(download_path, new_file_name)

        os.rename(downloaded_file, new_file_path)

        logger.info(f'Excel descargado exitosamente como {new_file_name}')

        df = pd.read_excel(new_file_path)
        return new_file_path
        
    except Exception as e:
        error_message = f'Error al intentar descargar excel: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)
    
def extract_and_save_table_semaforo(driver):
    logger.info('Intentando extraer y guardar datos de la tabla de semáforo')
    try:
        all_rows = []
        current_page = 1
        
        while True:
            # Esperar y obtener la tabla
            table = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-bordered"))
            )
            time.sleep(2)
            
            # Obtener datos de la página actual
            html_content = table.get_attribute('outerHTML')
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extraer filas - Agregamos más logging para debug
            tbody = soup.find('tbody')
            rows_in_current_page = 0  # Contador para esta página
            
            if tbody:
                all_trs = tbody.find_all('tr')
                logger.info(f'Encontradas {len(all_trs)} filas en página {current_page}')
                
                # for tr in all_trs:
                #     cells = tr.find_all('td')
                #     if len(cells) >= 8:
                #         try:
                #             row_data = [
                #                 cells[3].text.strip() if cells[3].text.strip() else "-",  # ANALISTA
                #                 cells[4].text.strip() if cells[4].text.strip() else "-",  # HORARIO
                #                 cells[5].text.strip() if cells[5].text.strip() else "-",  # MODALIDAD
                #                 cells[6].text.strip() if cells[6].text.strip() else "-",  # FECHA
                #                 cells[7].text.strip() if cells[7].text.strip() else "-"   # HORA DE INGRESO
                #             ]
                #             all_rows.append(row_data)
                #             rows_in_current_page += 1
                #         except Exception as cell_error:
                #             logger.warning(f'Error al procesar fila: {str(cell_error)}')
                #             continue
                rows = driver.find_elements(By.CSS_SELECTOR, "table.table-bordered tbody tr")
                rows_in_current_page = 0
                for row in rows:
                    try:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if len(cells) >= 8:
                            row_data = [
                                cells[3].text.strip() or "-",
                                cells[4].text.strip() or "-",
                                cells[5].text.strip() or "-",
                                cells[6].text.strip() or "-",
                                cells[7].text.strip() or "-"
                            ]
                            if any(field != "-" for field in row_data):
                                all_rows.append(row_data)
                                rows_in_current_page += 1
                    except Exception as cell_error:
                        logger.warning(f'Error al procesar fila: {str(cell_error)}')
                    continue
            
            logger.info(f'Procesada página {current_page} - Filas extraídas en esta página: {rows_in_current_page}')
            logger.info(f'Total de filas acumuladas hasta ahora: {len(all_rows)}')
            
            # Intentar encontrar el siguiente número
            next_page = current_page + 1
            try:
                # Primero verificar si el botón existe
                next_button = driver.find_elements(By.XPATH, 
                    f"//ul[contains(@class, 'pagination')]//a[normalize-space(.)='{next_page}']"
                )
                
                if not next_button:
                    logger.info(f'No se encontró botón para página {next_page}. Finalizando extracción.')
                    break
                
                # Si encontramos el botón, hacer clic
                next_button[0].click()
                current_page += 1
                time.sleep(2)  # Aumentamos el tiempo de espera
                
            except Exception as e:
                logger.info(f'No se pudo navegar a la siguiente página. Finalizando extracción: {str(e)}')
                break
        
        # Crear DataFrame
        headers = ['ANALISTA', 'HORARIO', 'MODALIDAD', 'FECHA', 'HORA DE INGRESO']
        df = pd.DataFrame(all_rows, columns=headers)
        
        logger.info(f'Total final de registros procesados: {len(df)}')
        
        # Guardar a Excel
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
    #extract_and_save_table_semaforo(driver)
    click_descargar_excel(driver, fecha_desde, fecha_hasta)





















    

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



