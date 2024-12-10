from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from  ...utils.input_utils  import random_delay
import time
from datetime import datetime, timedelta
from config import URL_NEW_CALL_CENTER
from utils.logger_config import get_newcallcenter_logger

logger = get_newcallcenter_logger()


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

def click_descargar(driver):
    try:
        logger.info('Intentando hacer clic en botón Descarga Aquí')

        # Usando la clase btn-primary
        download_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "descargarExcel"))
        )
        
        download_button.click()
        logger.info('Clic en Descarga Aquí exitoso')
        return True

    except Exception as e:
        error_message = f'Error al hacer clic en Descarga Aquí: {str(e)}'
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
    click_descargar(driver)
    

    

    



