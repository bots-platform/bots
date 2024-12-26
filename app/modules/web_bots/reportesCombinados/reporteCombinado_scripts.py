import asyncio
from concurrent.futures import ThreadPoolExecutor
import time
import pandas as pd
import xlrd
from datetime import datetime
import os
from ..semaforo.service import SemaforoService
from ..newCallCenter.service import NewCallCenterService
from ..sharepoint.service import SharepointService
from utils.logger_config import get_reporteCombinado_logger
from ..sharepoint.scripts import horario_General_ATCORP
from ..semaforo.scripts import semaforo_dataframe
from ..newCallCenter.scripts import newCallCenter_dataframe

logger = get_reporteCombinado_logger()

def generar_reporte_combinado(self, fecha_inicio, fecha_fin):
    try:
        logger.info("generando reportes combinados")

        semaforo_df = semaforo_dataframe.get_info_from_semaforo_downloaded_to_dataframe(fecha_inicio, fecha_fin)
        newcallCenter_clean_df = newCallCenter_dataframe.get_info_from_newcallcenter_download_to_dataframe(fecha_inicio, fecha_fin)
        df_sharepoint_horario_General_ATCORP = horario_General_ATCORP.get_info_from_Exel_saved_to_dataframe()
        
        merged_df = pd.merge(
            semaforo_df,
            newcallCenter_clean_df, 
            left_on=['Usuario', 'FECHA'],
            right_on=['Usuario', 'Fecha'],
            how='inner'
            )
        df_semaforo_ncc = pd.DataFrame({
            'CUENTA': "",                
            'AGENTES': merged_df['Usuario'].str.upper(),
            'FECHA': merged_df['FECHA'].dt.strftime('%d/%m/%Y'),
            'HORARIO LABORAL': merged_df['HORARIO'],
            'NCC': merged_df['HoraEntrada'],
            'SEMAFORO': merged_df['LOGUEO/INGRESO'],
            'RESPONSABLE': "" ,
            'TARDANZA NCC': "" ,
            'TARDANZA SEMAFORO': "" ,
            'CANTIDAD NCC': "" ,
            'CANTIDAD SEMAFORO': "" 
                         
        })
        df_semaforo_ncc['Nombre Normalizado'] = df_semaforo_ncc['AGENTES'].apply(
         lambda x: ' '.join([x.split()[0], x.split()[2]]) if len(x.split()) >= 3 else x
        )
        df_semaforo_ncc['Nombre Normalizado'] = df_semaforo_ncc['Nombre Normalizado'].str.upper()
        df_sharepoint_horario_General_ATCORP['Nombre'] = df_sharepoint_horario_General_ATCORP['Nombre'].str.upper()
        df_sharepoint_ncc_semaforo = pd.merge(
            df_semaforo_ncc,
            df_sharepoint_horario_General_ATCORP,
            left_on=['Nombre Normalizado', 'FECHA'],
            right_on=['Nombre', 'SOLO_FECHA'],
            how='inner'
        )
        df_sharepoint_ncc_semaforo['Horario Laboral Sharepoint'] = df_sharepoint_ncc_semaforo['Turno']
        
        try:
            save_info_obtained(df_semaforo_ncc, df_sharepoint_ncc_semaforo )

        except Exception as e:
            logger.error(f"Error al guardar el reporte combinado: {str(e)}")
            print(f"Error al guardar el reporte combinado: {str(e)}")
        return {
            "status": "success",
            "message": "Reporte combinado generado exitosamente",
        }

    except Exception as e:
        logger.error(f"Error al generar el reporte combinado: {str(e)}")
        print(f"Error al generar el reporte combinado: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }
    
def save_info_obtained(df_semaforo_ncc, df_sharepoint_ncc_semaforo ):

    output_dir = 'media/reportes_combinados'
    os.makedirs(output_dir, exist_ok=True) 

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    reporte_combinado_ruta = os.path.join(output_dir, f'df_semaforo_ncc{timestamp}.xlsx')
    df_semaforo_ncc.to_excel(reporte_combinado_ruta, index=False, engine='openpyxl')
    logger.info(f"Reporte Combinado guardado en: {reporte_combinado_ruta}")

    reporte_sharepoint_ncc_semaforo_ruta = os.path.join(output_dir, f'df_sharepoint_ncc_semaforo{timestamp}.xlsx')
    df_sharepoint_ncc_semaforo.to_excel(reporte_sharepoint_ncc_semaforo_ruta, index=False, engine='openpyxl')
    logger.info(f"Reporte Sharepoint-ncc-semaforo guardado en: {reporte_sharepoint_ncc_semaforo_ruta}")

    return reporte_sharepoint_ncc_semaforo_ruta
