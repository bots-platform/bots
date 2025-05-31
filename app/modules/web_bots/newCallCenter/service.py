from fastapi import HTTPException
from app.modules.web_bots.browser.setup import setup_edge_driver
from app.modules.web_bots.newCallCenter.scripts.newCallCenter_scraper import scrape_newcallcenter_page
from app.core.config import settings
import time
from utils.logger_config import get_newcallcenter_logger
import os
 
logger = get_newcallcenter_logger()

class NewCallCenterService:
    def descargarReporte(self,fecha_inicio, fecha_fin):
        try:
            driver = None
            if not settings.NEW_CALL_CENTER_USER or not settings.NEW_CALL_CENTER_PASSWORD:
                logger.error("New Call Center credenciales no encontradas .env file")
                return

            download_path = os.path.abspath("media/newcallcenter/")
       
            if not os.path.exists(download_path):
                os.makedirs(download_path)

            try:
                logger.info('Empezando scraping de New Call Center')
                driver = setup_edge_driver(download_directory=download_path)
                result = scrape_newcallcenter_page(driver, settings.NEW_CALL_CENTER_USER, settings.NEW_CALL_CENTER_PASSWORD, fecha_inicio, fecha_fin)
                return result

            except Exception as e:
                logger.error(f"Error en scraping de New Call Center: {str(e)}")
                return None

            finally:
                if driver:
                    driver.quit()
                    logger.info("NEW CALL CENTER CERRADO")

        except Exception as e:
           error_message = f" Error al descargar reporte: {str(e)}"
           logger.error(error_message)

           raise HTTPException(
                status_code=500,
                 detail=error_message
           )
