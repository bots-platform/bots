from fastapi import HTTPException
from app.modules.web_bots.browser.setup import setup_edge_driver
from app.modules.web_bots.semaforo.scripts.semaforo_scraper import scrape_semaforo_page
from config import SEMAFORO_USER, SEMAFORO_PASSWORD
import time
from utils.logger_config import get_semaforo_logger
import os

logger = get_semaforo_logger()

class SemaforoService:
    def descargarReporteWebScraping(self, fecha_inicio, fecha_fin):
        try:
            driver = None
            if not SEMAFORO_USER or not SEMAFORO_PASSWORD:
                logger.error("New Call Center credenciales no encontradas .env file")
                return
            
            download_path = os.path.abspath("media/semaforo/")
       
            if not os.path.exists(download_path):
                os.makedirs(download_path)

            try:        
                logger.info('Empezando scraping de SEMAFORO')
                driver = setup_edge_driver(download_directory=download_path)
                path_semaforo_report = scrape_semaforo_page(driver, SEMAFORO_USER, SEMAFORO_PASSWORD, fecha_inicio, fecha_fin)
                logger.info(f"Resultado del scraping: {path_semaforo_report}")
                return path_semaforo_report

            except Exception as e:
                logger.error(f"Error en scraping de SEMAFORO: {str(e)}")
                return None

            finally:
                if driver:
                    driver.quit()
                    logger.info("SEMAFORO CERRADO")

        except Exception as e:
           error_message = f" Error al descargar reporte: {str(e)}"
           logger.error(error_message)

           raise HTTPException(
                status_code=500,
                 detail=error_message
           )
