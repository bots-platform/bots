
from fastapi import APIRouter
from app.modules.web_bots.reportesCombinados.service import ReporteCombinadoService
from app.modules.web_bots.reportesCombinados.models import FechaReporteCombinadoRequest

router = APIRouter(prefix="/api/reportes", tags=["reportes_combinados"])

@router.post("/combinado")
def generar_reporte_combinado_endpoint(request: FechaReporteCombinadoRequest):
    print(request)
    print(request.fecha_inicio, request.fecha_fin)
    reporteCombinado_service = ReporteCombinadoService()
    response = reporteCombinado_service.generar_reporte_combinado(request.fecha_inicio, request.fecha_fin)
    return response

@router.get("/combinado")
def generate():
    return {'hola': 'reporteCombinado'}














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