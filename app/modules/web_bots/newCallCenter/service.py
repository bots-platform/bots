from fastapi import HTTPException
from app.modules.web_bots.browser.setup import setup_edge_driver
from app.modules.web_bots.newCallCenter.scripts.newCallCenter_scraper import scrape_newcallcenter_page
from config import NEW_CALL_CENTER_USER, NEW_CALL_CENTER_PASSWORD
import time
from utils.logger_config import get_newcallcenter_logger
import os
 
logger = get_newcallcenter_logger()


class NewCallCenterService:
    def descargarReporte(self,fecha_inicio, fecha_fin):
        try:
            driver = None
            if not NEW_CALL_CENTER_USER or not NEW_CALL_CENTER_PASSWORD:
                logger.error("New Call Center credenciales no encontradas .env file")
                return

            download_path = os.path.abspath("media/newcallcenter/")
       
            if not os.path.exists(download_path):
                os.makedirs(download_path)

            try:
                logger.info('Empezando scraping de New Call Center')
                driver = setup_edge_driver(download_directory=download_path)
                result = scrape_newcallcenter_page(driver, NEW_CALL_CENTER_USER, NEW_CALL_CENTER_PASSWORD, fecha_inicio, fecha_fin)
                while True:
                    try:
                        driver.current_url
                        time.sleep(1)
                    except Exception as e:
                        logger.info("El navegador ha sido cerrado.")
                        break

                return result
                # {
                #     "status": "success",
                #     "message": "Proceso completado. Verifica las descargas."
                # }
   
            except Exception as e:
                logger.error(f"Error en scraping de New Call Center: {str(e)}")

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
