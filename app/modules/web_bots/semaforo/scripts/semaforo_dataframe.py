from datetime import datetime
from ..service import SemaforoService
import xlrd
import pandas as pd
import os
from utils.logger_config import get_semaforo_logger


logger = get_semaforo_logger()

def get_info_from_semaforo_downloaded_to_dataframe(fecha_inicio, fecha_fin):

    semaforo_service = SemaforoService()
    semaforo_downloaded_path = semaforo_service.descargarReporteWebScraping(fecha_inicio, fecha_fin)

    if not semaforo_downloaded_path:
        raise ValueError("Error: `semaforo_path` es None. No se pudo descargar el reporte de Semaforo.")
    
    workbook = xlrd.open_workbook(semaforo_downloaded_path, ignore_workbook_corruption=True)

    semaforo_df = pd.read_excel(workbook)
    if semaforo_df is None:
        raise ValueError("Error al leer y pasar a dataframe el Semaforo downloaded")
    
    logger.info("Contenido del DataFrame Semaforo:")
    logger.info(semaforo_df.head())

    semaforo_df['FECHA'] = pd.to_datetime(semaforo_df['FECHA'], format='%Y-%m-%d')
    semaforo_df['LOGUEO/INGRESO'] = pd.to_datetime(semaforo_df['LOGUEO/INGRESO'], format='%H:%M:%S', errors='coerce')
    semaforo_df['LOGUEO/INGRESO'] = pd.to_datetime(semaforo_df['LOGUEO/INGRESO']).dt.strftime('%H:%M:%S')
    semaforo_df.rename(columns={'ANALISTA': 'Usuario'}, inplace=True)
    print("Semaforo DataFrame:")
    print(semaforo_df[['Usuario', 'FECHA']].head())
    #semaforo_df['Usuario_Semaforo'] = semaforo_df['Usuario'].str.strip().str.lower()
    semaforo_df['Fecha_Semaforo'] = pd.to_datetime(semaforo_df['FECHA'])
    semaforo_df['Usuario_Semaforo'] = semaforo_df['Usuario'].str.strip().apply(
    lambda x: ' '.join([x.split()[0], x.split()[2]]) if isinstance(x, str) and len(x.split()) == 4 else
              ' '.join([x.split()[0], x.split()[1]]) if isinstance(x, str) and len(x.split()) == 3 else
              x if isinstance(x, str) else ''
)


    #semaforo_df['Usuario_Semaforo'] = semaforo_df['Usuario_Semaforo'].str.upper()

    # Eliminar duplicados manteniendo solo la primera aparición
    semaforo_df = semaforo_df.drop_duplicates(subset=['Usuario_Semaforo', 'Fecha_Semaforo'], keep='first')


    save_info_obtained(semaforo_df)
    return semaforo_df
    
def save_info_obtained(semaforo_df):

    output_dir = 'media/reportes_combinados'
    os.makedirs(output_dir, exist_ok=True)  # Crear el directorio si no existe
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    reporte_semaforo = os.path.join(output_dir, f'semaforo_df_{timestamp}.xlsx')
    semaforo_df.to_excel(reporte_semaforo, index=False, engine='openpyxl')
    logger.info(f"Reporte Semaforo guardado en: {reporte_semaforo}")

    