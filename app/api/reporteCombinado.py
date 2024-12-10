
from fastapi import APIRouter
from app.modules.web_bots.semaforo.service import SemaforoService
from app.modules.web_bots.newCallCenter.service import NewCallCenterService
from app.modules.web_bots.semaforo.models import FechaReporteAsistenciaRequest

router = APIRouter(prefix="/api/reportes", tags=["reportes_combinados"])


from fastapi import APIRouter
from app.modules.web_bots.reportesCombinados.service import ReporteCombinadoService
from app.modules.web_bots.semaforo.models import FechaReporteAsistenciaRequest

router = APIRouter(prefix="/api/reportes", tags=["reportes_combinados"])

@router.post("/combinado")
async def generar_reporte_combinado_endpoint(request: FechaReporteAsistenciaRequest):
    return await ReporteCombinadoService.generar_reporte_combinado(
        request.fecha_inicio,
        request.fecha_fin
    )




















# @router.post("/combinado")
# async def descarga_reporte_combinado(request: FechaReporteAsistenciaRequest):
#     # Inicializar servicios
#     semaforo_service = SemaforoService()
#     newcallcenter_service = NewCallCenterService()
    
#     # Ejecutar ambos servicios
#     respuesta_semaforo = semaforo_service.descargarReporte(
#         request.fecha_inicio, 
#         request.fecha_fin
#     )
#     respuesta_newcallcenter = newcallcenter_service.descargarReporte(
#         request.fecha_inicio,
#         request.fecha_fin
#     )
    
#     # Combinar respuestas
#     return {
#         "semaforo": respuesta_semaforo,
#         "newcallcenter": respuesta_newcallcenter,
#         "status": "success",
#         "message": "Proceso combinado completado"
#     }