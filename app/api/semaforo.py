from fastapi import APIRouter
from app.modules.web_bots.semaforo.service import SemaforoService
from ..modules.web_bots.semaforo.models import FechaReporteAsistenciaRequest

router = APIRouter(prefix="/api/semaforo", tags=["semaforo"])

@router.post("/reporte")
def descarga_reporte(request: FechaReporteAsistenciaRequest):
    semaforo_service = SemaforoService()
    respone =  semaforo_service.descargarReporte(request.fecha_inicio, request.fecha_fin)
    print(respone)
    return respone

@router.get("/reporte")
def generate():
    return {'hola': 'semaforo'}