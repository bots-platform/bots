import asyncio
import pandas as pd
from datetime import datetime
import os
from ..semaforo.service import SemaforoService
from ..newCallCenter.service import NewCallCenterService


class ReporteCombinadoService:
    #@staticmethod
    async def generar_reporte_combinado(fecha_inicio: str, fecha_fin: str):
        try:
        
            for dir_path in ['media/semaforo/', 'media/newcallcenter/', 'media/reportes_combinados/']:
                os.makedirs(dir_path, exist_ok=True)


            semaforo_service = SemaforoService()
            newcallcenter_service = NewCallCenterService()


            tasks = [
                semaforo_service.descargarReporte(fecha_inicio, fecha_fin),
                newcallcenter_service.descargarReporte(fecha_inicio, fecha_fin)
            ]
            resultados = await asyncio.gather(*tasks)

            df_semaforo = resultados[0] 
            df_newcallcenter = resultados[1] 

            # excel_semaforo = resultados[0]
            # excel_newcallcenter = resultados[1]


            # df_semaforo = pd.read_excel(f'media/semaforo/{excel_semaforo}')
            # df_newcallcenter = pd.read_excel(f'media/newcallcenter/{excel_newcallcenter}')


            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            reporte_combinado = f'media/reportes_combinados/reporte_completo_{timestamp}.xlsx'

            with pd.ExcelWriter(reporte_combinado) as writer:
                df_semaforo.to_excel(writer, sheet_name='Semaforo', index=False)
                df_newcallcenter.to_excel(writer, sheet_name='NewCallCenter', index=False)

                # Aquí puedes agregar lógica adicional para combinar los datos

            return {
                "status": "success",
                "message": "Reporte combinado generado exitosamente",
                "file": reporte_combinado
            }

        except Exception as e:
            return {
                "status": "error",
                "message": str(e)
            }
