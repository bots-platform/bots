from datetime import datetime
from app.modules.web_bots.browser.setup_chrome import setup_chrome_driver
from app.modules.web_bots.sharepoint.scripts.sharepoint_scraper import scrape_sharepoint_page
from utils.logger_config import get_sharepoint_HorarioGeneralATCORP_logger
import win32com.client
import pandas as pd
import unicodedata

logger = get_sharepoint_HorarioGeneralATCORP_logger()

import os
from config import SHAREPOINT_PASSWORD, SHAREPOINT_USER
from fastapi import HTTPException

def guardar_excel_como():
   
    logger.info("Tratando de conectar con Excel Aplication")
    excel = win32com.client.Dispatch("Excel.Application")
    excel.Visible = True

    nombre_archivo = "Horario General ATcorp_2025.xlsx"

    for wb in excel.Workbooks:
        print(wb.Name)


    workbook = None

    for wb in excel.Workbooks:
            if wb.Name == nombre_archivo:
                workbook = wb
                break   

    #workbook = excel.ActiveWorkbook
    if not workbook:
        logger.error(f"No se encontro un archivo Excel abierto con el nombre '{nombre_archivo}")
        print(f"No se encontro un archivo Excel con el nombre '{nombre_archivo}")
        return None
    
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

    sharepointHorarioGeneral_path = guardar_excel_como()
    if not sharepointHorarioGeneral_path:
        raise ValueError("Error: `sharepointHorarioGeneral_path` es None. No se pudo descargar el reporte de sharepoint.")
    
    excel_data = pd.ExcelFile(sharepointHorarioGeneral_path)
    #hojas_seleccionadas = ['25-11 al 01-12', '02-12 al 08-12', '09-12 al 15-12', '16-12 al 22-12', '23-12 al 29-12', '30-12 al 05-01-25']
    hojas_seleccionadas = ['06-01 al 12-01']
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
                                'Fecha_General': encabezado,
                                'Usuario_General': nombre,
                                'Turno_General': turno,
                            })

    # Extraer la hora inicial de 'Turno_General'
    sharepoint_horario_General_ATCORP_df = pd.DataFrame(datos_extraidos)

    def extraer_hora_o_palabra(valor):
        if isinstance(valor, str) and ':' in valor:  
            return valor.split(' - ')[0].strip() 
        return valor  
    sharepoint_horario_General_ATCORP_df['Hora_Inicial_General'] = sharepoint_horario_General_ATCORP_df['Turno_General'].apply(extraer_hora_o_palabra)


    #sharepoint_horario_General_ATCORP_df['Hora_Inicial_General'] = sharepoint_horario_General_ATCORP_df['Turno_General'].str.extract(r'(\d{2}:\d{2})')
    sharepoint_horario_General_ATCORP_df['Fecha_General'] = pd.to_datetime(
        sharepoint_horario_General_ATCORP_df['Fecha_General'].str.extract(r'(\d{2}/\d{2}/\d{4})')[0],
        format='%d/%m/%Y',
        dayfirst=True,
        ).dt.strftime('%d/%m/%Y')
    sharepoint_horario_General_ATCORP_df['Usuario_General'] = sharepoint_horario_General_ATCORP_df['Usuario_General'].str.upper()



    def quitar_tildes(texto):
        if isinstance(texto, str):
            # Normaliza el texto y elimina caracteres combinados
            return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
        return texto
    sharepoint_horario_General_ATCORP_df['Usuario_General'] = sharepoint_horario_General_ATCORP_df['Usuario_General'].apply(quitar_tildes)

#     sharepoint_horario_General_ATCORP_df['Fecha_General'] = pd.to_datetime(
#     sharepoint_horario_General_ATCORP_df['Fecha_General'], format='%d/%m/%Y'
# ).dt.date

    duplicados_df = sharepoint_horario_General_ATCORP_df[
        sharepoint_horario_General_ATCORP_df.duplicated(subset=['Usuario_General', 'Fecha_General'], keep=False)
    ]
    
       # Imprimir los duplicados en consola
    print("Duplicados encontrados:")
    print(duplicados_df)
    print(f"Total de duplicados: {len(duplicados_df)}")
  
    duplicados_dir = 'media/horarioGeneralATCORP/duplicados'
    os.makedirs(duplicados_dir, exist_ok=True)  
    duplicados_path = os.path.join(
        duplicados_dir, f'duplicados_general_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    )
    duplicados_df.to_excel(duplicados_path, index=False)

    sharepoint_horario_General_ATCORP_df = sharepoint_horario_General_ATCORP_df.drop_duplicates(subset=['Usuario_General', 'Fecha_General'], keep='first')
    
    save_info_obtained(sharepoint_horario_General_ATCORP_df)
    
    return sharepoint_horario_General_ATCORP_df

def save_info_obtained(df):

    output_dir = 'media/sharepoint/horarioGeneralATCORP/reporte'
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    ruta_reporte_sharepoint = os.path.join(output_dir, f'df_sharepoint_horarioGeneralATCORP{timestamp}.xlsx')
    df.to_excel(ruta_reporte_sharepoint, index=False, engine='openpyxl')
    logger.info(f"Reporte Sharepoint General ATCORP guardado en: {ruta_reporte_sharepoint}")


    
















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

