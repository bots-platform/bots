from json import tool
import pandas as pd
from datetime import datetime
import os
from utils.logger_config import get_reporteCombinado_logger
from ..sharepoint.scripts import horario_general_atcorp, horario_mesa_atcorp
from ..semaforo.scripts import semaforo_dataframe
from ..newCallCenter.scripts import newCallCenter_dataframe

logger = get_reporteCombinado_logger()

def generar_reporte_combinado(fecha_inicio, fecha_fin):
    try:
        logger.info("generando reportes combinados")

        semaforo_df = semaforo_dataframe.get_info_from_semaforo_downloaded_to_dataframe(fecha_inicio, fecha_fin)
        newcallCenter_clean_df = newCallCenter_dataframe.get_info_from_newcallcenter_download_to_dataframe(fecha_inicio, fecha_fin)
        sharepoint_horario_general_atcorp_df = horario_general_atcorp.get_info_from_Exel_saved_to_dataframe()
        sharepoint_horario_mesa_atcorp_df = horario_mesa_atcorp.get_info_from_Excel_Saved()

        semaforo_df_renamed = semaforo_df.rename(columns={
            'Usuario_Semaforo': 'UsuarioC',
            'Fecha_Semaforo': 'FechaC',
            'LOGUEO/INGRESO':'Hora_SEMAFORO',
        })
        newcallCenter_clean_df_renamed = newcallCenter_clean_df.rename(columns={
            'Usuario_NCC': 'UsuarioC',
            'Fecha_NCC': 'FechaC',
            'HoraEntrada': 'Hora_NCC',
        })
        sharepoint_horario_general_atcorp_df_renamed = sharepoint_horario_general_atcorp_df.rename(columns={
            'Usuario_General': 'UsuarioC',
            'Fecha_General': 'FechaC',
            'Turno_General': 'Turno_General',

        })
        sharepoint_horario_mesa_atcorp_df_renamed = sharepoint_horario_mesa_atcorp_df.rename(columns={
            'Usuario_Mesa': 'UsuarioC',
            'Fecha_Mesa': 'FechaC',
            'Turno_Mesa': 'Turno_Mesa',
        })
    
    
        dataframes = [semaforo_df_renamed, newcallCenter_clean_df_renamed, sharepoint_horario_general_atcorp_df_renamed, sharepoint_horario_mesa_atcorp_df_renamed]
        for df in dataframes:
            df['UsuarioC'] = df['UsuarioC'].astype(str).str.strip()  
            df['FechaC'] = pd.to_datetime(df['FechaC'], errors='coerce')  

      
        # Crear un índice común para las combinaciones únicas de Usuario y Fecha
        all_users_dates = pd.concat([
            semaforo_df_renamed[['UsuarioC', 'FechaC']],
            newcallCenter_clean_df_renamed[['UsuarioC', 'FechaC']],
            sharepoint_horario_general_atcorp_df_renamed[['UsuarioC', 'FechaC']],
            sharepoint_horario_mesa_atcorp_df_renamed[['UsuarioC', 'FechaC']]
        ]).drop_duplicates().reset_index(drop=True)

        all_users_dates = all_users_dates.drop_duplicates().reset_index(drop=True)
  
        def reformat_name(nombre):
            if isinstance(nombre, str):
                partes = nombre.split()
                if len(partes) >= 3:
                    apellidos = " ".join(partes[-2:])  # Últimos dos como apellidos
                    nombres = " ".join(partes[:-2])  # Restante como nombres
                    return f"{apellidos} {nombres}"
                return nombre  # Devolver sin cambios si tiene menos de 3 partes
            return nombre

        try:
            final_combined_df = all_users_dates \
            .merge(semaforo_df_renamed, on=['UsuarioC', 'FechaC'], how='left', validate='one_to_many') \
            .merge(newcallCenter_clean_df_renamed, on=['UsuarioC', 'FechaC'], how='left', validate='one_to_many') \
            .merge(sharepoint_horario_general_atcorp_df_renamed, on=['UsuarioC', 'FechaC'], how='left', validate='one_to_many') \
            .merge(sharepoint_horario_mesa_atcorp_df_renamed, on=['UsuarioC', 'FechaC'], how='left', validate='one_to_many')

            final_combined_df['UsuarioC_formato'] = final_combined_df['Usuario_x'].apply(reformat_name)
            final_combined_df['FechaC_formato'] = pd.to_datetime(final_combined_df['FechaC']).dt.strftime('%d/%m/%Y')

        except KeyError as e:
            print(f"Error: {e}")

        try:
            path = save_info_obtained(final_combined_df)
            return  path

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
    
def save_info_obtained(df_sharepointATCORPGeneral_ncc_semaforo_SharepointMesaATCORP):

    output_dir = 'media/reportes_combinados/'
    os.makedirs(output_dir, exist_ok=True) 

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    df_sharepointATCORPGeneral_ncc_semaforo_SharepointMesaATCORP_ruta = os.path.join(output_dir, f'df_sharepointATCORPGeneral_ncc_semaforo_SharepointMesaATCORP{timestamp}.xlsx')
    df_sharepointATCORPGeneral_ncc_semaforo_SharepointMesaATCORP.to_excel(df_sharepointATCORPGeneral_ncc_semaforo_SharepointMesaATCORP_ruta, index=False, engine='openpyxl')
    logger.info(f"Reporte Combinado guardado en: {df_sharepointATCORPGeneral_ncc_semaforo_SharepointMesaATCORP_ruta}")

    return df_sharepointATCORPGeneral_ncc_semaforo_SharepointMesaATCORP_ruta

