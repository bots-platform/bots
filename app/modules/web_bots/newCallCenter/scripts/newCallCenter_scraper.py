from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from utils.waiting_download import wait_for_download
from  ...utils.input_utils  import random_delay
import time
from datetime import datetime, timedelta
from config import URL_NEW_CALL_CENTER
from utils.logger_config import get_newcallcenter_logger
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains

from bs4 import BeautifulSoup
import pandas as pd
import os

logger = get_newcallcenter_logger()

def handle_download_dialog(driver):
    try:
      
        download_icon = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[title='Downloads']"))
        )
        download_icon.click()
        
        
        keep_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[text()='Keep']"))
        )
        keep_button.click()
        
        logger.info("Descarga confirmada exitosamente")
        return True
        
    except Exception as e:
       
        try:
          
            ActionChains(driver)\
                .key_down(Keys.ALT).send_keys('j').key_up(Keys.ALT)\
                .pause(1)\
                .send_keys(Keys.TAB).send_keys(Keys.ENTER)\
                .perform()
            logger.info("Descarga confirmada usando atajos de teclado")
            return True
        except Exception as sub_e:
            logger.error(f"Error al manejar diálogo de descarga: {str(e)} - {str(sub_e)}")
            return False

def login_to_newcallcenter(driver, user, password):

    try:
        
        logger.info(f"Intentando login New Call Center")
        driver.get(URL_NEW_CALL_CENTER)
        random_delay(3, 5)  
        
        user_input = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.NAME, 'email'))
            )
        user_input.clear()
        for char in user:
            user_input.send_keys(char)
            time.sleep(0.1)

        password_input = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.NAME, 'password'))
            )
        password_input.clear()
        for char in password:
            password_input.send_keys(char)
            time.sleep(0.1) 

        loggin_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CLASS_NAME, 'btn-login'))
        )
        loggin_button.click()

        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, 'page-container'))
        )

        logger.info(f"login exitoso")
        return True
    
    except Exception as e:
        logger.error(f"Falló login {str(e)}")
        return False
    
def navegar_reportes_actividadAgente(driver):
    logger.info('Seleccionando opcion reportes Activaidad Agente')
    try:
        # Después del login exitoso
        time.sleep(1)
        driver.get("http://10.200.90.200/reportes/actividad-agente")
        logger.info('opcion Reporte Actividad Agente seleccionada exitosamente')
    except Exception as e:
        error_message = f'fallo al ingresar a reporte/Actividad-agente: { str (e)}'
        logger.error(error_message)
        raise Exception(error_message)

def set_fechas_newcallcenter(driver, fecha_inicio, fecha_fin):
    try:
        logger.info(f'Estableciendo fechas del calendario: desde {fecha_inicio} hasta {fecha_fin}')
        
       
        calendario_icon = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.ID, "default-daterange"))
        )
        
        calendario_icon.click()
        time.sleep(1)  
        
      
        fecha_input = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.NAME, "default-daterange"))
        )
        fecha_input.clear()
        fecha_input.send_keys(f"{fecha_inicio.strftime('%d/%m/%Y')} - {fecha_fin.strftime('%d/%m/%Y')}")
        
        calendario_icon = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.ID, "default-daterange"))
        )
        
        calendario_icon.click()
        time.sleep(1)
        aceptar_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "applyBtn"))
        )

        
        aceptar_button.click()
        
        logger.info('Fechas establecidas exitosamente')
        return True
        
    except Exception as e:
        error_message = f'Error al establecer fechas en el calendario: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)

def seleccionar_dropdown_agenteLoging(driver):
    logger.info('Tratando de hacer click en desplegable')
    try:
        time.sleep(2) 
    
        dropdown = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, 'span.select2-selection'))
        )
        time.sleep(1)
        dropdown.click()
        time.sleep(1)
    except Exception as e:
        error_message = f'Fallo hacer click en desplegable: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)

def choose_agenteLogin(driver):
    logger.info('Tratando de selecdcionar agenteLogin de la lista desplegable')
    try:
        agente_login_option = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, 'li[id*="AgentLogin"]'))
        )
        time.sleep(1)
        agente_login_option.click()
    except Exception as e:
        error_message = f'Fallo en seleccionar Agente Login de la lista desplegable: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)

def click_descargar(driver, fecha_desde, fecha_hasta):
    try:
        logger.info('Intentando hacer clic en botón Descarga Aquí')

        download_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "descargarExcel"))
        )
        download_button.click()
        download_path = os.path.abspath("media/newcallcenter/")

        downloaded_file = wait_for_download(download_path, timeout=60, polling_interval=1)

        if not downloaded_file:
            raise Exception("El archivo no se descargó dentro del tiempo de espera")

        logger.info(f"Archivo descargado encontrado: {downloaded_file}")

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        new_file_name = f"new__call_center{fecha_desde}_{fecha_hasta}_{timestamp}.xlsx"

        new_file_path = os.path.join(download_path, new_file_name)

        os.rename(downloaded_file, new_file_path)

        logger.info(f'Excel descargado exitosamente como {new_file_name}')
                    
        df = pd.read_excel(new_file_path)
        return new_file_path

    except Exception as e:
        error_message = f'Error al hacer clic en Descarga Aquí: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)

def click_boton_buscar(driver):
    try:
        logger.info("Intentando hacer click en botón buscar Agente")
        boton_buscar = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, 'buscarAgente'))
        )
        time.sleep(1)  
        boton_buscar.click()
        logger.info("Click exitoso en botón buscar")

    except Exception as e:
        error_message = f'Fallo al hacer click en botón buscar: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)    

def extract_and_save_table(driver):
    logger.info('Intentando extraer y guardar datos de la tabla')
    try:
        all_rows = []
        current_page = 1
        total_pages = None
        
        while True:
            
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "table.table-bordered"))
            )
            time.sleep(2)  
            
          
            if not total_pages:
                pagination_text = driver.find_element(By.CSS_SELECTOR, ".dataTables_info").text
                total_records = int(pagination_text.split()[-2].replace(',', ''))
                total_pages = (total_records + 49) // 50 
                logger.info(f'Total de registros a procesar: {total_records} en {total_pages} páginas')

           
            table = driver.find_element(By.CSS_SELECTOR, "table.table-bordered")
            html_content = table.get_attribute('outerHTML')
            soup = BeautifulSoup(html_content, 'html.parser')
            
           
            for tr in soup.find('tbody').find_all('tr'):
                cells = tr.find_all('td')
                if cells:
                    row_data = [cell.text.strip() for cell in cells]
                    all_rows.append(row_data)
            
            logger.info(f'Procesada página {current_page} de {total_pages}')
            
           
            if current_page >= total_pages:
                break
                
          
            next_button = driver.find_element(By.XPATH, "//a[text()='Siguiente']")
            if 'disabled' in next_button.get_attribute('class'):
                break
            next_button.click()
            current_page += 1
            time.sleep(2) 
        
      
        headers = ['Fecha', 'Anexo', 'Usuario', 'Agente', 'Evento', 'Duracion', 'Motivo']
        df = pd.DataFrame(all_rows, columns=headers)
        
     
        os.makedirs('media/newcallcenter', exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'media/newcallcenter/reporte_agentes_{timestamp}.xlsx'
        df.to_excel(filename, index=False)
        
        logger.info(f'Datos guardados exitosamente en {filename}. Total de registros: {len(df)}')
        return filename
        
    except Exception as e:
        error_message = f'Error al extraer/guardar datos de la tabla: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)

def scrape_newcallcenter_page(driver, user, password, fecha_inicio, fecha_fin):
 
    login_to_newcallcenter(driver, user, password)
    navegar_reportes_actividadAgente(driver)
    seleccionar_dropdown_agenteLoging(driver)
    choose_agenteLogin(driver)
    click_boton_buscar(driver)
    set_fechas_newcallcenter(driver,fecha_inicio,fecha_fin)
    click_boton_buscar(driver)
    path_newcallcenter = click_descargar(driver, fecha_inicio, fecha_fin)
    return path_newcallcenter
    #extract_and_save_table(driver)

 





    



