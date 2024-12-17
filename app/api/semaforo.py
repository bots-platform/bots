from fastapi import APIRouter
from fastapi.responses import FileResponse
from app.modules.web_bots.semaforo.service import SemaforoService
from ..modules.web_bots.semaforo.models import FechaReporteAsistenciaRequest

router = APIRouter(prefix="/api/semaforo", tags=["semaforo"])

@router.post("/reporte")
def descarga_reporte(request: FechaReporteAsistenciaRequest):
    semaforo_service = SemaforoService()
    ruta_archivo =  semaforo_service.descargarReporte(request.fecha_inicio, request.fecha_fin)
    print(ruta_archivo)
    return FileResponse(
        ruta_archivo,
        filename=ruta_archivo.split("/")[-1],
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

# @router.get("/reporte")
# def generate():
#     return {'hola': 'semaforo'}