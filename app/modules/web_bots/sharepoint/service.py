from datetime import datetime
from fastapi import HTTPException
import win32com.client
from app.modules.web_bots.sharepoint.scripts.sharepoint_scraper import scrape_sharepoint_page 
from app.modules.web_bots.browser.setup_chrome import setup_chrome_driver
from config import SHAREPOINT_USER, SHAREPOINT_PASSWORD
import time
from utils.logger_config import get_sharepoint_logger
import os
 
logger = get_sharepoint_logger()

class SharepointService:
    def descargarReporte(self):
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

    def guardar_excel_como(self):
        # Conectar con la aplicación de Excel
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = True  # Mostrar Excel para ver el proceso (puedes poner False para ocultarlo)
    
        # Obtener el workbook activo
        workbook = excel.ActiveWorkbook
    
        carpeta_destino = os.path.abspath("media/sharepoint")
    
        if not os.path.exists(carpeta_destino):
                os.makedirs(carpeta_destino)
    
        # Ruta completa para guardar el archivo
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre_Horario_General =  f'Horario_General_{timestamp}.xlsx'
        
        ruta_guardado = os.path.join(carpeta_destino, nombre_Horario_General)

        try:
            # Guardar una copia en la carpeta especificada
            workbook.SaveAs(ruta_guardado)
            print(f"Archivo guardado en: {ruta_guardado}")
            
            #workbook.Close(SaveChanges=False)
            return ruta_guardado
    
        except Exception as e:
            print(f"Error al guardar el archivo: {e}")
            return None
        finally:
            # Cerrar el workbook sin cerrar Excel
            # workbook.Close(SaveChanges=False)  # Descomenta si deseas cerrar el archivo después de guardar
            pass
        
