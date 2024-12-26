from datetime import datetime
import os
from ..service import NewCallCenterService
from utils.logger_config import get_newcallcenter_logger
import pandas as pd

logger = get_newcallcenter_logger()

def get_info_from_newcallcenter_download_to_dataframe(fecha_inicio, fecha_fin):

    newcallcenter_service = NewCallCenterService() 
    newcallcenter_downloaded_path = newcallcenter_service.descargarReporte(fecha_inicio, fecha_fin)

    if not newcallcenter_downloaded_path:
        raise ValueError("Error: `newcallcenter_path` es None. No se pudo descargar el reporte de NewCallCenter.")
    newcallcenter_df = pd.read_excel(newcallcenter_downloaded_path, skiprows=6,  engine='openpyxl')

    if newcallcenter_df is None:
        raise ValueError("Error al leer y pasar a dataframe de NewCallCenter")
    
    logger.info("\nContenido del DataFrame NewCallCenter:")
    logger.info(newcallcenter_df.head())

    newcallcenter_df['Fecha'] = pd.to_datetime(newcallcenter_df['Fecha'], format='%d/%m/%Y %H:%M:%S')
    newcallcenter_df['Día'] = newcallcenter_df['Fecha'].dt.date
    newcallcenter_clean_df = newcallcenter_df.loc[newcallcenter_df.groupby(['Usuario', 'Día'])['Fecha'].idxmin()]
    newcallcenter_clean_df = newcallcenter_clean_df.drop(columns=['Día'])
    newcallcenter_clean_df['Fecha'] = pd.to_datetime(newcallcenter_clean_df['Fecha'], format='%d/%m/%Y')
    newcallcenter_clean_df['HoraEntrada'] = newcallcenter_clean_df['Fecha'].dt.strftime('%H:%M:%S')
    # Convertir la columna 'Fecha' a solo fecha (sin hora)
    newcallcenter_clean_df['Fecha'] = pd.to_datetime(newcallcenter_clean_df['Fecha']).dt.date
    print("NewCallCenter DataFrame:")
    print(newcallcenter_clean_df[['Usuario', 'Fecha']].head())
    newcallcenter_clean_df['Usuario'] = newcallcenter_clean_df['Usuario'].str.strip().str.lower()
    newcallcenter_clean_df['Fecha'] = pd.to_datetime(newcallcenter_clean_df['Fecha'])
    print(newcallcenter_clean_df['Fecha'].dtype)
    save_info_obtained(newcallcenter_clean_df)

    return newcallcenter_clean_df

def save_info_obtained(newcallCenter_clean_df):

    output_dir = 'media/reportes_combinados'
    os.makedirs(output_dir, exist_ok=True)  # Crear el directorio si no existe
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    reporte_newcallcenter = os.path.join(output_dir, f'newcallcenter_df_{timestamp}.xlsx')
    newcallCenter_clean_df.to_excel(reporte_newcallcenter, index=False, engine='openpyxl')
    logger.info(f"Reporte NewCallCenter guardado en: {reporte_newcallcenter}")

    