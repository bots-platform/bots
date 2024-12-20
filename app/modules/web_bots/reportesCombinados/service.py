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

logger = get_reporteCombinado_logger()

class ReporteCombinadoService:

    def generar_reporte_combinado(self, fecha_inicio, fecha_fin):
        try:
            logger.info("generando reportes combinados")

            semaforo_service = SemaforoService()
            newcallcenter_service = NewCallCenterService() 
            sharepoint_service = SharepointService()

            semaforo_path = semaforo_service.descargarReporte(fecha_inicio, fecha_fin)
            if not semaforo_path:
                raise ValueError("Error: `semaforo_path` es None. No se pudo descargar el reporte de Semaforo.")

            newcallcenter_path = newcallcenter_service.descargarReporte(fecha_inicio, fecha_fin)
            if not newcallcenter_path:
                raise ValueError("Error: `newcallcenter_path` es None. No se pudo descargar el reporte de NewCallCenter.")
           
            sharepoint_path = sharepoint_service.guardar_excel_como()
            if not sharepoint_path:
                raise ValueError("Error: `sharepoint_path` es None. No se pudo descargar el reporte de sharepoint.")

            workbook = xlrd.open_workbook(semaforo_path, ignore_workbook_corruption=True)
            semaforo_df = pd.read_excel(workbook)
            if semaforo_df is None:
                raise ValueError("Error al leer y pasar a dataframe Semaforo")
            logger.info("Contenido del DataFrame Semaforo:")
            logger.info(semaforo_df.head())

            newcallcenter_df = pd.read_excel(newcallcenter_path, skiprows=6,  engine='openpyxl')  # Saltar las 6 primeras filas
            if newcallcenter_df is None:
                raise ValueError("Error al leer y pasar a dataframe de NewCallCenter")
            logger.info("\nContenido del DataFrame NewCallCenter:")
            logger.info(newcallcenter_df.head())

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

            newcallcenter_df['Fecha'] = pd.to_datetime(newcallcenter_df['Fecha'], format='%d/%m/%Y %H:%M:%S')
            newcallcenter_df['Día'] = newcallcenter_df['Fecha'].dt.date
            newcallcenter_clean_df = newcallcenter_df.loc[newcallcenter_df.groupby(['Usuario', 'Día'])['Fecha'].idxmin()]
            newcallcenter_clean_df = newcallcenter_clean_df.drop(columns=['Día'])

            semaforo_df['FECHA'] = pd.to_datetime(semaforo_df['FECHA'], format='%Y-%m-%d')
            semaforo_df['LOGUEO/INGRESO'] = pd.to_datetime(semaforo_df['LOGUEO/INGRESO'], format='%H:%M:%S', errors='coerce')
            semaforo_df['LOGUEO/INGRESO'] = pd.to_datetime(semaforo_df['LOGUEO/INGRESO']).dt.strftime('%H:%M:%S')

            newcallcenter_clean_df['Fecha'] = pd.to_datetime(newcallcenter_clean_df['Fecha'], format='%d/%m/%Y')
            newcallcenter_clean_df['HoraEntrada'] = newcallcenter_clean_df['Fecha'].dt.strftime('%H:%M:%S')
            # Convertir la columna 'Fecha' a solo fecha (sin hora)
            newcallcenter_clean_df['Fecha'] = pd.to_datetime(newcallcenter_clean_df['Fecha']).dt.date

            semaforo_df.rename(columns={'ANALISTA': 'Usuario'}, inplace=True)

            print("Semaforo DataFrame:")
            print(semaforo_df[['Usuario', 'FECHA']].head())

            print("NewCallCenter DataFrame:")
            print(newcallcenter_clean_df[['Usuario', 'Fecha']].head())

            semaforo_df['Usuario'] = semaforo_df['Usuario'].str.strip().str.lower()
            newcallcenter_clean_df['Usuario'] = newcallcenter_clean_df['Usuario'].str.strip().str.lower()

            # Convertir ambas columnas a datetime64[ns]
            semaforo_df['FECHA'] = pd.to_datetime(semaforo_df['FECHA'])
            newcallcenter_clean_df['Fecha'] = pd.to_datetime(newcallcenter_clean_df['Fecha'])

            print(semaforo_df['FECHA'].dtype)
            print(newcallcenter_clean_df['Fecha'].dtype)

            merged_df = pd.merge(
                semaforo_df,
                newcallcenter_clean_df, 
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
            logger.info(df_semaforo_ncc)
            print(df_semaforo_ncc.head())
            logger.info(df_semaforo_ncc.head())

            df_semaforo_ncc['Nombre Normalizado'] = df_semaforo_ncc['Nombre Normalizado'].str.upper()
            df_sharepoint['Nombre'] = df_sharepoint['Nombre'].str.upper()

            df_sharepoint_ncc_semaforo = pd.merge(
                df_semaforo_ncc,
                df_sharepoint,
                left_on=['Nombre Normalizado', 'FECHA'],
                right_on=['Nombre', 'SOLO_FECHA'],
                how='inner'
            )
            #df_sharepoint_ncc_semaforo['HORARIO LABORAL'] = df_sharepoint_ncc_semaforo['Turno'],
            df_sharepoint_ncc_semaforo['Horario Laboral Sharepoint'] = df_sharepoint_ncc_semaforo['Turno']
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            try:

                output_dir = 'media/reportes_combinados'
                os.makedirs(output_dir, exist_ok=True)  # Crear el directorio si no existe

                reporte_semaforo = os.path.join(output_dir, f'semaforo_df_{timestamp}.xlsx')
                semaforo_df.to_excel(reporte_semaforo, index=False, engine='openpyxl')
                logger.info(f"Reporte Semaforo guardado en: {reporte_semaforo}")

                reporte_newcallcenter = os.path.join(output_dir, f'newcallcenter_df_{timestamp}.xlsx')
                newcallcenter_clean_df.to_excel(reporte_newcallcenter, index=False, engine='openpyxl')
                logger.info(f"Reporte NewCallCenter guardado en: {reporte_newcallcenter}")

                reporte_combinado = os.path.join(output_dir, f'df_semaforo_ncc{timestamp}.xlsx')
                df_semaforo_ncc.to_excel(reporte_combinado, index=False, engine='openpyxl')
                logger.info(f"Reporte Combinado guardado en: {reporte_combinado}")

                reporte_sharepoint = os.path.join(output_dir, f'df_sharepoint{timestamp}.xlsx')
                df_sharepoint.to_excel(reporte_sharepoint, index=False, engine='openpyxl')
                logger.info(f"Reporte Sharepoint guardado en: {reporte_sharepoint}")

                reporte_sharepoint_ncc_semaforo = os.path.join(output_dir, f'df_sharepoint_ncc_semaforo{timestamp}.xlsx')
                df_sharepoint_ncc_semaforo.to_excel(reporte_sharepoint_ncc_semaforo, index=False, engine='openpyxl')
                logger.info(f"Reporte Sharepoint-ncc-semaforo guardado en: {reporte_sharepoint_ncc_semaforo}")

                return reporte_sharepoint_ncc_semaforo


            
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
