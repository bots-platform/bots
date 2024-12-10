from fastapi import APIRouter
from app.modules.web_bots.newCallCenter.models import FechaReporteActividadAgenteRequest
from app.modules.web_bots.newCallCenter.service import NewCallCenterService

router = APIRouter(prefix="/api/newcallcenter", tags=["newcallcenter"])

@router.post("/reporte")
def descarga_reporte(request: FechaReporteActividadAgenteRequest):
    NewCallCenter_service = NewCallCenterService()
    respone =  NewCallCenter_service.descargarReporte(request.fecha_inicio, request.fecha_fin)
    print(respone)
    return respone

@router.get("/reporte")
def generate():
    return {'hola': 'newCallCenters'}