from datetime import datetime
from app.modules.web_bots.browser.setup_chrome import setup_chrome_driver
from app.modules.web_bots.sharepoint.scripts.sharepoint_scraper import scrape_sharepoint_page
from utils.logger_config import get_sharepoint_logger
import win32com.client
import pandas as pd

logger = get_sharepoint_logger()

import os
from config import SHAREPOINT_PASSWORD, SHAREPOINT_USER
from fastapi import HTTPException

def guardar_excel_como():
   
    logger.info("Tratando de conectar con Excel Aplication")
    excel = win32com.client.Dispatch("Excel.Application")
    excel.Visible = True

    workbook = excel.ActiveWorkbook

    carpeta_destino = os.path.abspath("media/sharepoint/horarioGeneralATCORP/downloads")

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

def get_info_from_Exel_saved_to_dataframe():

    sharepoint_path = guardar_excel_como()
    if not sharepoint_path:
        raise ValueError("Error: `sharepoint_path` es None. No se pudo descargar el reporte de sharepoint.")
    
    excel_data = pd.ExcelFile(sharepoint_path)
    hojas_seleccionadas = ['28-10 al 03-11', '04-11 al 10-11', '11-11 al 17-11', '18-11 al 24-11', '25-11 al 01-12', '02-12 al 08-12', '09-12 al 15-12', '16-12 al 22-12']
    datos_extraidos = []

    for hoja in hojas_seleccionadas:
        sharepoint_df = pd.read_excel(excel_data, sheet_name=hoja, header=None)
        fila_referencia = 0
        fila_inicio = sharepoint_df[0].first_valid_index()
        for i in range(fila_inicio, len(sharepoint_df)):
            if pd.isnull(sharepoint_df.iloc[i,0]):
                fila_referencia = i + 2
                break

        encabezados_dias = sharepoint_df.iloc[0,2:].dropna().tolist()

        for i, row  in sharepoint_df.iterrows():
            if i >=fila_referencia and pd.notnull(row[1]):
                nombre= row[1]  
                if nombre not in ["Turno", "Personal FO/BO"]:
                    for idx, encabezado in enumerate(encabezados_dias):
                        turno_col= 2 + idx * 3
                        turno = row[turno_col]
                        if pd.notnull(turno):
                            datos_extraidos.append({
                                'Fecha': encabezado,
                                'Nombre': nombre,
                                'Turno': turno,
                            })
    df_sharepoint = pd.DataFrame(datos_extraidos)
    df_sharepoint['SOLO_FECHA'] = pd.to_datetime(df_sharepoint['Fecha'].str.extract(r'(\d{2}/\d{2}/\d{4})')[0],format='%d/%m/%Y').dt.strftime('%d/%m/%Y')
    df_sharepoint.head(10)
    save_info_obtained(df_sharepoint)
    return df_sharepoint

def save_info_obtained(df):

    output_dir = 'media/sharepoint/horarioGeneralATCORP/reporte'
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    ruta_reporte_sharepoint = os.path.join(output_dir, f'df_sharepoint_horarioGeneralATCORP{timestamp}.xlsx')
    df.to_excel(ruta_reporte_sharepoint, index=False, engine='openpyxl')
    logger.info(f"Reporte Sharepoint guardado en: {ruta_reporte_sharepoint}")


    













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

