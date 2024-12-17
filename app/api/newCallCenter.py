from fastapi import APIRouter
from fastapi.responses import FileResponse
from app.modules.web_bots.newCallCenter.models import FechaReporteActividadAgenteRequest
from app.modules.web_bots.newCallCenter.service import NewCallCenterService

router = APIRouter(prefix="/api/newcallcenter", tags=["newcallcenter"])

@router.post("/reporte")
def descarga_reporte(request: FechaReporteActividadAgenteRequest):
    NewCallCenter_service = NewCallCenterService()
    ruta_archivo =  NewCallCenter_service.descargarReporte(request.fecha_inicio, request.fecha_fin)
    print(ruta_archivo)
    return FileResponse(
        ruta_archivo,
        filename=ruta_archivo.split("/")[-1],
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

# @router.get("/reporte")
# def generate():
#     return {'hola': 'newCallCenters'}