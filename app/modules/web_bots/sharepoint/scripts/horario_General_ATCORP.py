from datetime import datetime
from app.modules.web_bots.browser.setup_chrome import setup_chrome_driver
from app.modules.web_bots.sharepoint.scripts.sharepoint_scraper import scrape_sharepoint_page
from utils.logger_config import get_sharepoint_logger
import win32com.client

logger = get_sharepoint_logger()

import os
from config import SHAREPOINT_PASSWORD, SHAREPOINT_USER
from fastapi import HTTPException

def guardar_excel_como():
   
    logger.info("Tratando de conectar con Excel Aplication")
    excel = win32com.client.Dispatch("Excel.Application")
    excel.Visible = True 
    workbook = excel.ActiveWorkbook
    carpeta_destino = os.path.abspath("media/sharepoint")
    if not os.path.exists(carpeta_destino):
            os.makedirs(carpeta_destino)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    nombre_Horario_General =  f'Horario_General_{timestamp}.xlsx'
    
    ruta_guardado = os.path.join(carpeta_destino, nombre_Horario_General)
    try:
        logger.info("Tratando de guardar conectar con Excel Aplication")
        workbook.SaveAs(ruta_guardado)
        print(f"Archivo guardado en: {ruta_guardado}")
        
        #workbook.Close(SaveChanges=False)
        return ruta_guardado

    except Exception as e:
        logger.error(f"Error tratando de guardar el archivo excel: {e}")
        print(f"Error al guardar el archivo: {e}")
        return None
    finally:
        # Cerrar el workbook sin cerrar Excel
        # workbook.Close(SaveChanges=False)  # Descomenta si deseas cerrar el archivo después de guardar
        pass

def descargarReporteSelenium(self):
    try:
        driver = None
        if not SHAREPOINT_USER or not SHAREPOINT_PASSWORD:
            logger.error("Sharepoint credenciales no encontradas .env file")
            return
        download_path = os.path.abspath("media/sharepoint/")
   
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        try:
            logger.info('Empezando scraping de Sharepoint')
            driver = setup_chrome_driver(download_directory=download_path)
            result = scrape_sharepoint_page(driver, SHAREPOINT_USER, SHAREPOINT_PASSWORD)
            return result
        except Exception as e:
            logger.error(f"Error en scraping de Sharepoint: {str(e)}")
            return None
        finally:
            if driver:
                #driver.quit()
                logger.info("SHAREPOINT CERRADO")
    except Exception as e:
       error_message = f" Error al descargar reporte: {str(e)}"
       logger.error(error_message)
       raise HTTPException(
            status_code=500,
             detail=error_message
       )

