from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from utils.waiting_download import wait_for_download
from  ...utils.input_utils  import random_delay
import time
from datetime import datetime
from config import URL_SHAREPOINT
from utils.logger_config import get_sharepoint_HorarioGeneralATCORP_logger
import pandas as pd
import os

logger = get_sharepoint_HorarioGeneralATCORP_logger()

def login_to_sharepoint(driver, user, password):

    try:
        
        logger.info(f"Intentando login Sharepoint")
        driver.get(URL_SHAREPOINT)
        random_delay(3, 5)  
        
        user_input = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.ID, 'i0116'))
            )
        user_input.clear()
        for char in user:
            user_input.send_keys(char)
            time.sleep(0.1)

        boton_siguiente = WebDriverWait(driver, 20).until(
                 EC.element_to_be_clickable((By.XPATH, '(//input[@id="idSIButton9"])[1]'))
            )
        boton_siguiente.click()

    
        password_input = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.ID, 'i0118'))
            )
        password_input.clear()
        
        for char in password:
            password_input.send_keys(char)
            time.sleep(0.1) 

        loggin_button = WebDriverWait(driver, 30).until(
        EC.element_to_be_clickable((By.XPATH, '//input[@id="idSIButton9" and @type="submit"]'))
        )
        loggin_button.click()


        logger.info(f"login exitoso")
        return True
    
    except Exception as e:
        logger.error(f"Falló login {str(e)}")
        return False

def navegar_sharepoint_horarioGeneralATCORP(driver):
    logger.info('Navegando sharepoint horario General ATCORP')
    try:
        # Después del login exitoso
        time.sleep(1)
        driver.get(URL_SHAREPOINT)
        logger.info('sharepoint horario General ATCORP seleccionada exitosamente')
    except Exception as e:
        error_message = f'fallo al ingresar a sharepoint horario General ATCORP: { str (e)}'
        logger.error(error_message)
        raise Exception(error_message)

def seleccionar_archivo(driver):
    logger.info('Tratando de hacer click en archivo')
    try:
        time.sleep(2) 
    
        dropdown = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(By.XPATH, "//span[text()='Archivo']")
        )
        time.sleep(1)
        dropdown.click()
        time.sleep(1)
    except Exception as e:
        error_message = f'Fallo hacer click en archivo: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)

def seleccionar_crear_copia(driver):
    logger.info('Tratando de hacer click en crear copia en linea')
    try:
        time.sleep(2) 
    
        dropdown = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(By.XPATH, "//span[text()='Crear una copia en línea']")
        )
        time.sleep(1)
        dropdown.click()
        time.sleep(1)
    except Exception as e:
        error_message = f'Fallo hacer click en crear una copia en linea: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)
    
def seleccionar_descargar_copia(driver):
    logger.info('Tratando de hacer click en descargar copia')
    try:
        time.sleep(2) 
    
        dropdown = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(By.XPATH, "//span[text()='Descargar una copia']")
        )
        time.sleep(1)
        dropdown.click()
        download_path = os.path.abspath("media/sharepoint/")

        downloaded_file = wait_for_download(download_path, timeout=60, polling_interval=1)

        if not downloaded_file:
            raise Exception("El archivo no se descargó dentro del tiempo de espera")

        logger.info(f"Archivo sharepoint descargado encontrado: {downloaded_file}")

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        new_file_name = f"sharepoint_{timestamp}.xlsx"

        new_file_path = os.path.join(download_path, new_file_name)

        os.rename(downloaded_file, new_file_path)

        logger.info(f'Excel sharepoint descargado exitosamente como {new_file_name}')
                    
        df = pd.read_excel(new_file_path)
        return new_file_path

    except Exception as e:
        error_message = f'Error al hacer clic en Descarga sharepoint Aquí: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)

def scrape_sharepoint_page(driver, user, password):
    
    login_to_sharepoint(driver, user, password)
    navegar_sharepoint_horarioGeneralATCORP(driver)
    seleccionar_archivo(driver)
    seleccionar_crear_copia(driver)
    path_sharepoint = seleccionar_descargar_copia(driver)
    return path_sharepoint

 





    



