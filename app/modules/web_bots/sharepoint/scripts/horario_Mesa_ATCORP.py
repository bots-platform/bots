from utils.logger_config import get_sharepoint_HorarioMesaATCORP
import win32com.client
import os
from datetime import datetime
import pandas as pd

logger = get_sharepoint_HorarioMesaATCORP()

def save_from_Sync_Desktop_Excel():
      
       logger.info("Tratando de conectar con Excel Aplication")
       excel = win32com.client.Dispatch("Excel.Application")
       excel.Visible = True 
       workbook = excel.ActiveWorkbook
       carpeta_destino = os.path.abspath("media/sharepoint/horarioMesaATCORP/downloads")

       if not os.path.exists(carpeta_destino):
               os.makedirs(carpeta_destino)

       timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
       nombre_Horario_General =  f'Horario_MesaATCORP_{timestamp}.xlsx'

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
       
def get_info_from_Excel_Saved():

    ruta_archivo = save_from_Sync_Desktop_Excel()
    excel_data = pd.ExcelFile(ruta_archivo)
    hojas_seleccionadas = ['25 nov - 01 dic', '02 dic - 08 dic ', '09 dic - 15 dic ', '16 dic - 22 dic  '] 
    datos_extraidos = []

    for hoja in hojas_seleccionadas:
        sharepoint_df = pd.read_excel(excel_data, sheet_name=hoja, header=None)

        fechas = sharepoint_df.iloc[0, 3:].dropna().tolist()

        for i, row in sharepoint_df.iterrows():
            if i >= 2 and pd.notnull(row[0]):
                analista = row[0] 

                for idx, fecha in enumerate(fechas):
                    turno_col = 3 + idx * 2  
                    turno = row[turno_col]

                    if pd.notnull(turno):
                        datos_extraidos.append({
                            'Fecha': fecha,
                            'Analista': analista,
                            'Turno': turno,
                        })
    df_resultado = pd.DataFrame(datos_extraidos)
    print(df_resultado.head())
    return df_resultado

def save_info_obtained():

    output_dir = 'media/sharepoint/horarioMesaATCORP/reporte'
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    df_sharepoint_horarioMesaATCORP = get_info_from_Excel_Saved()
    ruta_reporte_sharepoint = os.path.join(output_dir, f'df_sharepoint_horarioMesaATCORP{timestamp}.xlsx')
    df_sharepoint_horarioMesaATCORP.to_excel(ruta_reporte_sharepoint, index=False, engine='openpyxl')
    logger.info(f"Reporte Sharepoint horarioMesaATCORP  guardado en: {ruta_reporte_sharepoint}")

    return ruta_reporte_sharepoint

