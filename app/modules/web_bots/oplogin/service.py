from fastapi import HTTPException
from app.modules.web_bots.browser.setup import setup_chrome_driver
from app.modules.web_bots.oplogin.scripts.oplogin_scraper import scrape_oplogin_page
from config import OPLOGIN_USER, OPLOGIN_PASSWORD
import time
from utils.logger_config import get_oplogin_logger
import os

logger = get_oplogin_logger()


class OploginService:
    def descargarReporte(self):
        try:
            driver = None
            if not OPLOGIN_USER or not OPLOGIN_PASSWORD:
                logger.error("Oplogin credenciales no encontradas .env file")
                return
            
            download_path = os.path.abspath("media/oplogin/")
       
            if not os.path.exists(download_path):
                os.makedirs(download_path)
            try:
                logger.info('Empezando scraping de Oplogin')
                driver = setup_chrome_driver(download_directory=download_path)
                result = scrape_oplogin_page(driver, OPLOGIN_USER, OPLOGIN_PASSWORD)
                while True:
                    try:
                        driver.current_url
                        time.sleep(1)
                    except Exception as e:
                        logger.info("El navegador ha sido cerrado.")
                        break

                return {
                    "status": "success",
                    "message": "Proceso completado. Verifica las descargas."
                }
   
            except Exception as e:
                logger.error(f"Error en scraping de Oplogin: {str(e)}")

            finally:
                if driver:
                    driver.quit()
                    logger.info("OPLOGIN CERRADO")

        except Exception as e:
           error_message = f" Error al descargar reporte: {str(e)}"
           logger.error(error_message)

           raise HTTPException(
                status_code=500,
                 detail=error_message
           )
