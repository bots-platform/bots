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
        # Después del login exitoso
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
        fecha_desde_input.send_keys(fecha_desde.strftime("%m/%d/%Y"))
        
        time.sleep(1)
       
        fecha_hasta_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//label[text()='Fecha(Hasta)']/following-sibling::input"))
        )
        fecha_hasta_input.clear()
        fecha_hasta_input.send_keys(fecha_hasta.strftime("%m/%d/%Y"))
        
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

        download_path = "media/semaforo/"
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        driver = setup_chrome_driver(download_directory=download_path)
        time.sleep(2)

        logger.info(f'Excel descargado exitosamente en {download_path}')
        return True
        
    except Exception as e:
        error_message = f'Error al intentar descargar excel: {str(e)}'
        logger.error(error_message)
        raise Exception(error_message)
 

def scrape_semaforo_page(driver, user, password, fecha_desde, fecha_hasta):
 
    login_to_semaforo(driver, user, password)
    navegar_reportes_asistencia(driver)
    set_fechas_filtro(driver, fecha_desde, fecha_hasta)
    click_filtrar(driver)
    click_descargar_excel(driver)

    

    



