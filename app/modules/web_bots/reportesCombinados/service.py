import asyncio
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from datetime import datetime
import os
from ..semaforo.service import SemaforoService
from ..newCallCenter.service import NewCallCenterService
from utils.logger_config import get_reporteCombinado_logger

logger = get_reporteCombinado_logger()

class ReporteCombinadoService:

    def generar_reporte_combinado(self, fecha_inicio, fecha_fin):
        try:
            logger.info("generando reportes combinados")

            semaforo_service = SemaforoService()
            newcallcenter_service= NewCallCenterService() 

            semaforo_path = semaforo_service.descargarReporte(fecha_inicio, fecha_fin)
            newcallcenter_path = newcallcenter_service.descargarReporte(fecha_inicio, fecha_fin)

            semaforo_df = pd.read_excel(semaforo_path,  engine='xlrd')
            newcallcenter_df = pd.read_excel(newcallcenter_path, skiprows=6,  engine='openpyxl')  # Saltar las 6 primeras filas
               
            if semaforo_df is None:
                raise ValueError("Error al descargar el reporte de Semaforo")
            
            if newcallcenter_df is None:
                raise ValueError("Error al descargar el reporte de NewCallCenter")
            
           
            logger.info("Contenido del DataFrame Semaforo:")
            logger.info(semaforo_df.head())

            logger.info("\nContenido del DataFrame NewCallCenter:")
            logger.info(newcallcenter_df.head())
            
            # Limpiar duplicados del DataFrame de NewCallCenter
            newcallcenter_df['Fecha'] = pd.to_datetime(newcallcenter_df['Fecha'], format='%d/%m/%Y %H:%M:%S')
            newcallcenter_df['Día'] = newcallcenter_df['Fecha'].dt.date
            newcallcenter_clean_df = newcallcenter_df.loc[newcallcenter_df.groupby(['Usuario', 'Día'])['Fecha'].idxmin()]
            newcallcenter_clean_df = newcallcenter_clean_df.drop(columns=['Día'])
            #newcallcenter_clean_df = newcallcenter_clean_df.iloc[6:]

            
            semaforo_df['FECHA'] = pd.to_datetime(semaforo_df['FECHA'])
            semaforo_df['LOGUEO/INGRESO'] = pd.to_datetime(semaforo_df['LOGUEO/INGRESO']).dt.strftime('%H:%M:%S')

            newcallcenter_clean_df['Fecha'] = pd.to_datetime(newcallcenter_clean_df['Fecha'])
            newcallcenter_clean_df['HoraEntrada'] = newcallcenter_clean_df['Fecha'].dt.strftime('%H:%M:%S')

           
            semaforo_df.rename(columns={'ANALISTA': 'Usuario'}, inplace=True)

            # Unir los DataFrames por 'Usuario' y 'Fecha'
            merged_df = pd.merge(semaforo_df, newcallcenter_clean_df, left_on=['Usuario', 'FECHA'], right_on=['Usuario', 'Fecha'], how='inner')

            # Crear el tercer DataFrame con las columnas deseadas
            final_df = pd.DataFrame({
                'CUENTA': merged_df['#'],                  # O ajusta si necesitas una columna específica
                'AGENTES': merged_df['Usuario'],
                'FECHA': merged_df['FECHA'].dt.strftime('%d/%m/%Y'),
                'HORARIO LABORAL': merged_df['HORARIO'],
                'NCC': merged_df['HoraEntrada'],
                'SEMAFORO': merged_df['LOGUEO/INGRESO'],
                'RESPONSABLE': merged_df['AGENTE']         # Ajusta si esta columna es diferente en tus datos
            })

            # Mostrar el DataFrame final
            logger.info(final_df)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            reporte_combinado = f'media/reportes_combinados/reporte_completo_{timestamp}.xlsx'

            print(final_df.head())
            logger.info(final_df.head())


            try:
                final_df.to_excel(reporte_combinado, index=False, engine='openpyxl')
                logger.info(f"Reporte combinado guardado en: {reporte_combinado}")
                print(f"Reporte combinado guardado en: {reporte_combinado}")
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
