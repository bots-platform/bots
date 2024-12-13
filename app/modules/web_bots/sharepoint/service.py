from fastapi import HTTPException
from app.modules.web_bots.sharepoint.scripts.sharepoint_scraper import scrape_sharepoint_page 
from app.modules.web_bots.browser.setup import setup_edge_driver
from config import SHAREPOINT_USER, SHAREPOINT_PASSWORD
import time
from utils.logger_config import get_newcallcenter_logger
import os
 
logger = get_newcallcenter_logger()


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
                driver = setup_edge_driver(download_directory=download_path)
                result = scrape_sharepoint_page(driver, SHAREPOINT_USER, SHAREPOINT_PASSWORD)
                return result

            except Exception as e:
                logger.error(f"Error en scraping de Sharepoint: {str(e)}")
                return None

            finally:
                if driver:
                    driver.quit()
                    logger.info("SHAREPOINT CERRADO")

        except Exception as e:
           error_message = f" Error al descargar reporte: {str(e)}"
           logger.error(error_message)

           raise HTTPException(
                status_code=500,
                 detail=error_message
           )
