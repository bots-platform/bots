from datetime import datetime
from fastapi import HTTPException
#import win32com.client
from app.modules.web_bots.browser.setup_chrome import setup_chrome_driver
from config import SHAREPOINT_USER, SHAREPOINT_PASSWORD
import time
from utils.logger_config import get_sharepoint_HorarioGeneralATCORP_logger
import os
import pandas as pd
 
logger = get_sharepoint_HorarioGeneralATCORP_logger()

class SharepointService:

    def guardar_excel_como(self):
       
        logger.info("Tratando de conectar con Excel Aplication")

        carpeta_destino = os.path.abspath("media/sharepoint")

        if not os.path.exists(carpeta_destino):
                os.makedirs(carpeta_destino)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        nombre_Horario_General =  f'Horario_General_{timestamp}.xlsx'
        
        ruta_guardado = os.path.join(carpeta_destino, nombre_Horario_General)

        try:
            logger.info("Tratando de guardar conectar con Excel Aplication")
            print(f"Archivo guardado en: {ruta_guardado}")
            
            return ruta_guardado
    
        except Exception as e:
            logger.error(f"Error tratando de guardar el archivo excel: {e}")
            print(f"Error al guardar el archivo: {e}")
            return None
        finally:
            pass









        
