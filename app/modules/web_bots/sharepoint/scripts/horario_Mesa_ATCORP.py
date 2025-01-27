import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

from utils.logger_config import get_sharepoint_HorarioMesaATCORP
import win32com.client
import os
from datetime import datetime
import pandas as pd
import unicodedata

logger = get_sharepoint_HorarioMesaATCORP()

def save_from_Sync_Desktop_Excel():
      
       logger.info("Tratando de conectar con Excel Aplication")
       excel = win32com.client.Dispatch("Excel.Application")
       excel.Visible = True 

       nombre_archivo = "HORARIO MESA ATCORP.xlsx"

       workbook = None

       for wb in excel.Workbooks:
            if wb.Name == nombre_archivo:
                 workbook = wb
                 break 

       if not workbook:
            logger.error(f"No se encontro un archivo Excel abierto con el nombre '{nombre_archivo}")
            print(f"No se encontro un archivo Excel con el nombre '{nombre_archivo}")
            return None
       
       carpeta_destino = os.path.abspath("media/sharepoint/horarioMesaATCORP/downloads")

       if not os.path.exists(carpeta_destino):
               os.makedirs(carpeta_destino)

       timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
       nombre_Horario_MesaATCORP =  f'Horario_MesaATCORP_{timestamp}.xlsx'

       ruta_guardado = os.path.join(carpeta_destino, nombre_Horario_MesaATCORP)
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

    sharepointHorarioGeneral_path = save_from_Sync_Desktop_Excel()
    if not sharepointHorarioGeneral_path:
       raise ValueError("Error: `sharepointHorarioGeneral_path` es None. No se pudo descargar el reporte de sharepoint.")


    excel_data = pd.ExcelFile(sharepointHorarioGeneral_path)
    #hojas_seleccionadas = ['25 nov - 01 dic', '02 dic - 08 dic ', '09 dic - 15 dic ', '16 dic - 22 dic  ', '23 dic - 29 dic  ','30 dic - 05  En'] 
    hojas_seleccionadas = ['06 Ene - 12 Ene'] 
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
                            'Fecha_Mesa': fecha,
                            'Usuario_Mesa': analista,
                            'Turno_Mesa': turno,
                        })

    # Extraer la hora inicial de 'Turno_General'
    sharepoint_horario_Mesa_ATCORP_df = pd.DataFrame(datos_extraidos)

    def extraer_hora_o_palabra(valor):
        if isinstance(valor, str) and ':' in valor:  
            return valor.split(' - ')[0].strip() 
        return valor  
    sharepoint_horario_Mesa_ATCORP_df['Hora_Inicial_Mesa'] = sharepoint_horario_Mesa_ATCORP_df['Turno_Mesa'].apply(extraer_hora_o_palabra)


    #sharepoint_horario_Mesa_ATCORP_df['Hora_Inicial_Mesa'] = sharepoint_horario_Mesa_ATCORP_df['Turno_Mesa'].str.extract(r'(\d{2}:\d{2})')
    sharepoint_horario_Mesa_ATCORP_df['Fecha_Mesa'] = pd.to_datetime(
    sharepoint_horario_Mesa_ATCORP_df['Fecha_Mesa'].str.extract(r'(\d{1,2}/\d{1,2}/\d{4})')[0],
    format='%d/%m/%Y'
)
    sharepoint_horario_Mesa_ATCORP_df['Usuario_Mesa'] = sharepoint_horario_Mesa_ATCORP_df['Usuario_Mesa'].str.upper()
    
    # Función para eliminar tildes
    def quitar_tildes(texto):
        if isinstance(texto, str):
            # Normaliza el texto y elimina caracteres combinados
            return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
        return texto
    sharepoint_horario_Mesa_ATCORP_df['Usuario_Mesa'] = sharepoint_horario_Mesa_ATCORP_df['Usuario_Mesa'].apply(quitar_tildes)

    # sharepoint_horario_Mesa_ATCORP_df['Fecha_Mesa'] = pd.to_datetime(
    # sharepoint_horario_Mesa_ATCORP_df['Fecha_Mesa'], format='%d/%m/%Y'
    # ).dt.date


    duplicados_df = sharepoint_horario_Mesa_ATCORP_df[
        sharepoint_horario_Mesa_ATCORP_df.duplicated(subset=['Usuario_Mesa', 'Fecha_Mesa'], keep=False)
    ]

    # Imprimir los duplicados en consola
    print("Duplicados encontrados:")
    print(duplicados_df)
    print(f"Total de duplicados: {len(duplicados_df)}")

    duplicados_dir = 'media/sharepoint/horarioMesaATCORP/duplicados'
    os.makedirs(duplicados_dir, exist_ok=True) 
    duplicados_path = os.path.join(
        duplicados_dir, f'duplicados_horarioMesaATCORP_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    )
    duplicados_df.to_excel(duplicados_path, index=False)

    sharepoint_horario_Mesa_ATCORP_df = sharepoint_horario_Mesa_ATCORP_df.drop_duplicates(
        subset=['Usuario_Mesa', 'Fecha_Mesa'], keep='first'
    )

    save_info_obtained(sharepoint_horario_Mesa_ATCORP_df)

    return sharepoint_horario_Mesa_ATCORP_df        

def save_info_obtained(df):

    output_dir = 'media/sharepoint/horarioMesaATCORP/reporte'
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    ruta_reporte_sharepoint = os.path.join(output_dir, f'df_sharepoint_horarioMesaATCORP{timestamp}.xlsx')
    df.to_excel(ruta_reporte_sharepoint, index=False, engine='openpyxl')
    logger.info(f"Reporte Sharepoint horarioMesaATCORP  guardado en: {ruta_reporte_sharepoint}")
  


