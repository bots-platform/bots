from json import tool
import pandas as pd
from datetime import datetime
import os
from utils.logger_config import get_reporteCombinado_logger
from ..sharepoint.scripts import horario_General_ATCORP, horario_Mesa_ATCORP
from ..semaforo.scripts import semaforo_dataframe
from ..newCallCenter.scripts import newCallCenter_dataframe

logger = get_reporteCombinado_logger()

def generar_reporte_combinado(fecha_inicio, fecha_fin):
    try:
        logger.info("generando reportes combinados")

        semaforo_df = semaforo_dataframe.get_info_from_semaforo_downloaded_to_dataframe(fecha_inicio, fecha_fin)
        newcallCenter_clean_df = newCallCenter_dataframe.get_info_from_newcallcenter_download_to_dataframe(fecha_inicio, fecha_fin)
        sharepoint_horario_General_ATCORP_df = horario_General_ATCORP.get_info_from_Exel_saved_to_dataframe()
        sharepoint_horario_Mesa_ATCORP_df = horario_Mesa_ATCORP.get_info_from_Excel_Saved()

         # Verificar contenido de DataFrames antes del renombrado
        print("Contenido de sharepoint_horario_Mesa_ATCORP_df antes de renombrar:")
        print(sharepoint_horario_Mesa_ATCORP_df.head())
        print("Columnas originales:", sharepoint_horario_Mesa_ATCORP_df.columns)


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
        sharepoint_horario_General_ATCORP_df_renamed = sharepoint_horario_General_ATCORP_df.rename(columns={
            'Usuario_General': 'UsuarioC',
            'Fecha_General': 'FechaC',
            'Turno_General': 'Turno_General',

        })
        sharepoint_horario_Mesa_ATCORP_df_renamed = sharepoint_horario_Mesa_ATCORP_df.rename(columns={
            'Usuario_Mesa': 'UsuarioC',
            'Fecha_Mesa': 'FechaC',
            'Turno_Mesa': 'Turno_Mesa',
        })
        print("Columnas después de renombrar:")
        print(sharepoint_horario_Mesa_ATCORP_df_renamed.columns)

        # Verificar si está vacío
        if sharepoint_horario_Mesa_ATCORP_df_renamed.empty:
            logger.error("El DataFrame sharepoint_horario_Mesa_ATCORP_df_renamed está vacío.")
            raise ValueError("El DataFrame sharepoint_horario_Mesa_ATCORP_df_renamed está vacío.")


         # Verificar si algún DataFrame está vacío
        dfs = [semaforo_df_renamed, newcallCenter_clean_df_renamed, sharepoint_horario_General_ATCORP_df_renamed, sharepoint_horario_Mesa_ATCORP_df_renamed]
        for df, name in zip(dfs, ['semaforo', 'newCallCenter', 'General', 'Mesa']):
            if df.empty:
                logger.error(f"El DataFrame {name} está vacío.")
                raise ValueError(f"El DataFrame {name} está vacío.")
        
         # Verificar duplicados en las claves (Usuario, Fecha)
        for df, name in zip(dfs, ['semaforo', 'newCallCenter', 'General', 'Mesa']):
            print(f"Verificando duplicados en {name}:")
            print(df.duplicated(subset=['UsuarioC', 'FechaC']).sum())
            df.reset_index(drop=True, inplace=True)
        
        dfs = [semaforo_df_renamed, newcallCenter_clean_df_renamed, sharepoint_horario_General_ATCORP_df_renamed, sharepoint_horario_Mesa_ATCORP_df_renamed]
        names = ['Semáforo', 'CallCenter Limpio', 'Horario General', 'Horario Mesa']

        for df, name in zip(dfs, names):
            duplicados = df[df.duplicated(subset=['UsuarioC', 'FechaC'], keep=False)]
            print(f"Registros duplicados en {name}:")
            duplicados.to_excel(f'duplicados_{name}.xlsx', index=False)
            print(duplicados)

             # Eliminar duplicados manteniendo solo la primera aparición
            # df_sin_duplicados = df.drop_duplicates(subset=['Usuario', 'Fecha'], keep='first')
            # print(f"DataFrame {name} después de eliminar duplicados:")
            # print(df_sin_duplicados.head())  # Verificar los resultados
        
        
        #semaforo_df_renamed = semaforo_df_renamed.drop_duplicates(subset=['Usuario', 'Fecha'], keep=False)


          # Verificar columnas antes del merge
        for df, name in zip(
            [semaforo_df_renamed, newcallCenter_clean_df_renamed, sharepoint_horario_General_ATCORP_df_renamed, sharepoint_horario_Mesa_ATCORP_df_renamed],
            ['semaforo_df_renamed', 'newcallCenter_clean_df_renamed', 'sharepoint_horario_General_ATCORP_df_renamed', 'sharepoint_horario_Mesa_ATCORP_df_renamed']
        ):
            if 'UsuarioC' not in df.columns or 'FechaC' not in df.columns:
                logger.error(f"El DataFrame {name} no contiene las columnas 'Usuario' o 'Fecha'")
                raise KeyError(f"El DataFrame {name} no contiene las columnas 'Usuario' o 'Fecha'")
            
       
        # Convertir 'UsuarioC' a string y 'FechaC' a datetime en todos los DataFrames
        dataframes = [semaforo_df_renamed, newcallCenter_clean_df_renamed, sharepoint_horario_General_ATCORP_df_renamed, sharepoint_horario_Mesa_ATCORP_df_renamed]
        for df in dataframes:
            df['UsuarioC'] = df['UsuarioC'].astype(str).str.strip()  # Convertir a string
            df['FechaC'] = pd.to_datetime(df['FechaC'], errors='coerce')  # Convertir a datetime

        for df, name in zip(dataframes, ['Semaforo', 'CallCenter', 'General', 'Mesa']):
            print(f"Duplicados en {name}:")
            print(df.duplicated(subset=['UsuarioC', 'FechaC']).sum())  # Imprimir duplicados
            df.drop_duplicates(subset=['UsuarioC', 'FechaC'], inplace=True)  # Eliminar duplicados


        # Crear un índice común para las combinaciones únicas de Usuario y Fecha
        all_users_dates = pd.concat([
            semaforo_df_renamed[['UsuarioC', 'FechaC']],
            newcallCenter_clean_df_renamed[['UsuarioC', 'FechaC']],
            sharepoint_horario_General_ATCORP_df_renamed[['UsuarioC', 'FechaC']],
            sharepoint_horario_Mesa_ATCORP_df_renamed[['UsuarioC', 'FechaC']]
        ]).drop_duplicates().reset_index(drop=True)

        # Verificar contenido de all_users_dates
        print("Contenido de all_users_dates:")
        print(all_users_dates.head())  # Muestra las primeras filas
        print("Número de combinaciones únicas de Usuario y Fecha:", len(all_users_dates))

         # Verifica duplicados en claves combinadas
        all_users_dates = all_users_dates.drop_duplicates().reset_index(drop=True)

        print("Duplicados en sharepoint_horario_Mesa_ATCORP_df_renamed:")
        print(sharepoint_horario_Mesa_ATCORP_df_renamed.duplicated(subset=['UsuarioC', 'FechaC']).sum())

        # Hacer un merge alineando todos los DataFrames con el índice común
        try:
            final_combined_df = all_users_dates \
            .merge(semaforo_df_renamed, on=['UsuarioC', 'FechaC'], how='left', validate='one_to_many') \
            .merge(newcallCenter_clean_df_renamed, on=['UsuarioC', 'FechaC'], how='left', validate='one_to_many') \
            .merge(sharepoint_horario_General_ATCORP_df_renamed, on=['UsuarioC', 'FechaC'], how='left', validate='one_to_many') \
            .merge(sharepoint_horario_Mesa_ATCORP_df_renamed, on=['UsuarioC', 'FechaC'], how='left', validate='one_to_many')
        except KeyError as e:
            print(f"Error: {e}")    

        #tool.display_dataframe_to_user(name="Efficiently Combined DataFrame", dataframe=final_combined_df)

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
