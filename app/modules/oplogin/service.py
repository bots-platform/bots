from fastapi import HTTPException
from app.modules.oplogin.browser.setup import setup_chrome_driver
from app.modules.oplogin.scripts.oplogin_scraper import scrape_oplogin_page
from dotenv import load_dotenv
import os
import time
import logging  

class OploginService:
    def descargarReporte(self):
        try:
            load_dotenv()
            driver = None
            user = os.getenv('OPLOGIN_USER')
            password = os.getenv('OPLOGIN_PASSWORD')

            if not user or not password:
                logging.error("Oplogin credenciales no encontradas .env file")
                return
            try:
                logging.info('Empezando scraping de Oplogin')
                driver = setup_chrome_driver()
                result = scrape_oplogin_page(driver, user, password)
                while True:
                    try:
                        driver.current_url
                        time.sleep(1)
                    except Exception as e:
                        self.logger.info("El navegador ha sido cerrado.")
                        break

                return {
                    "status": "success",
                    "message": "Proceso completado. Verifica las descargas."
                }
   
            except Exception as e:
                logging.error(f"Error en scraping de Oplogin: {str(e)}")

            finally:
                if driver:
                    driver.quit()
                    logging.info("OPLOGIN CERRADO")

        except Exception as e:
           error_message = f" Error al descargar reporte: {str(e)}"
           logging.error(error_message)

           raise HTTPException(
                status_code=500,
                 detail=error_message
           )
