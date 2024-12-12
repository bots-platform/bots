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

    async def generar_reporte_combinado(self, fecha_inicio, fecha_fin):
        try:
            logger.info("generando reportes combinados")

            semaforo_service = SemaforoService()
            newcallcenter_service= NewCallCenterService()

            loop = asyncio.get_running_loop()

            with ThreadPoolExecutor(max_workers=2) as executor:
      
                tasks = [
                    loop.run_in_executor(executor, semaforo_service.descargarReporte, fecha_inicio, fecha_fin),
                    loop.run_in_executor(executor, newcallcenter_service.descargarReporte, fecha_inicio, fecha_fin)
                ]

                resultados = await asyncio.gather(*tasks)

                semaforo_df, newcallcenter_df = resultados


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
            df_newcallcenter_clean = newcallcenter_df.loc[newcallcenter_df.groupby(['Usuario', 'Día'])['Fecha'].idxmin()]
            df_newcallcenter_clean = df_newcallcenter_clean.drop(columns=['Día'])
            df_newcallcenter_clean = df_newcallcenter_clean.iloc[6:]

            
            semaforo_df['FECHA'] = pd.to_datetime(semaforo_df['FECHA'])
            semaforo_df['LOGUEO/INGRESO'] = pd.to_datetime(semaforo_df['LOGUEO/INGRESO']).dt.strftime('%H:%M:%S')

            df_newcallcenter_clean['Fecha'] = pd.to_datetime(df_newcallcenter_clean['Fecha'])
            df_newcallcenter_clean['HoraEntrada'] = df_newcallcenter_clean['Fecha'].dt.strftime('%H:%M:%S')

           
            semaforo_df.rename(columns={'ANALISTA': 'Usuario'}, inplace=True)

            # Unir los DataFrames por 'Usuario' y 'Fecha'
            merged_df = pd.merge(semaforo_df, df_newcallcenter_clean, left_on=['Usuario', 'FECHA'], right_on=['Usuario', 'Fecha'], how='inner')

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

            # Guardar el DataFrame en un archivo Excel
            final_df.to_excel('reporte_combinado.xlsx', index=False)
            return True

   
        
        except Exception as e:
            logger.error(f"Error al generar el reporte combinado: {str(e)}")
            print(f"Error al generar el reporte combinado: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
