from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

from app.modules.web_bots.browser.setup import setup_chrome_driver
from  ....web_bots.utils.input_utils  import random_delay
import time
from datetime import datetime, timedelta
from config import URL_OPLOGIN
from utils.logger_config import get_oplogin_logger
import os


logger = get_oplogin_logger()



def login_to_oplogin(driver, user, password):

    try:
        
        logger.info(f"Intentando login OPLOGIN")
        driver.get(URL_OPLOGIN)
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
            EC.element_to_be_clickable((By.ID, 'btnLoginSubmit'))
        )
        loggin_button.click()
        logger.info(f"login exitoso")
        return True
    
    except Exception as e:
        logger.error(f"Falló login {str(e)}")
        return False

def select_opcion_Severity_down(driver):
    
    logger.info('Seleccionando opcion Severity')
    try:
        dropdown = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR,'button.multiselect.dropdown-toggle'))
        )
        dropdown.click()
        logger.info("Dropdown abierto")
        time.sleep(0.5)
        
        down_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH,"//li//label[normalize-space()='INTERMIT']"))
        )
        down_option.click()
        logger.info("Opción DOWN seleccionada")
        
      
        selected_text = dropdown.text.strip()
        if "DOWN" in selected_text:
            logger.info("Verificación exitosa: DOWN está seleccionado")
            dropdown.click()
            logger.info("Cerrando lista Drop DOWN ")
            
            return True
        else:
            logger.warning("Verificación falló: DOWN no aparece seleccionado")
            return False
            
    except Exception as e:
        logger.error(f"Error seleccionando DOWN: {str(e)}")
        return False

def selec_calendario_lastUpdate(driver):
    logger.info('Seleccionando Calendario')
    try:
       
        date_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, 
                "date_range"))
        )
        date_input.click()
        
        custom_range = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, 
                "//li[@data-range-key='Custom Range']"))
        )
        custom_range.click()
        
        logger.info("Custom Range seleccionado exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error seleccionando Rango personalizado: {str(e)}")
        return False

def set_fechInicio_fechaFin(driver):
    try:
        today = datetime.now()
        week_ago = today - timedelta(days=7)
           
        date_input = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='date_range']"))
        )
        date_input.click()
        
        custom_range = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//li[@data-range-key='Custom Range']"))
        )
        custom_range.click()
        
        start_date_input = driver.find_element(By.NAME, "daterangepicker_start")
        start_date_input.clear()
        start_date_input.send_keys(week_ago.strftime("%m/%d/%Y"))
        
        end_date_input = driver.find_element(By.NAME, "daterangepicker_end")
        end_date_input.clear()
        end_date_input.send_keys(today.strftime("%m/%d/%Y"))
        
        apply_button = driver.find_element(By.CLASS_NAME, "applyBtn")
        apply_button.click()
        
        return True
        
    except Exception as e:
        logger.error(f"Error estableciendo el rango de fechas: {str(e)}")
        return False

def click_boton_dropdown_export(driver):
    try:
        logger.info("Tratando clickear en boton lista desplegable")
        dropdown_toggle = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 
                "button.btn.btn-primary.dropdown-toggle[data-toggle='dropdown']"))
        )
        dropdown_toggle.click()
    except Exception as e:
        raise(f"Error en click sobre boton lista desplegable: {str(e)}")
    
def click_btn_ExportExcel(driver):
    try:
        logger.info("Intentando click en boton Exportar a excel")
        export_option = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "btnExport"))
        )
        export_option.click()
        time.sleep(8)

        logger.info(f'Excel descargado exitosamente')
        return True

    except Exception as e:
        logger.error(f"Error dando click en boton Exportar a excel: {str(e)}")
    
def scrape_oplogin_page(driver, user, password):
 
    login_to_oplogin(driver, user, password)
    select_opcion_Severity_down(driver)
    selec_calendario_lastUpdate(driver)
    set_fechInicio_fechaFin(driver)
    click_boton_dropdown_export(driver)
    click_btn_ExportExcel(driver)

    

    



