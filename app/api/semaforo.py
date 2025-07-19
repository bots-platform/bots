import os
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from app.modules.web_bots.semaforo.service import SemaforoService
from ..modules.web_bots.semaforo.models import FechaReporteAsistenciaRequest

router = APIRouter(prefix="/api/semaforo", tags=["semaforo"])

@router.post("/reporte", include_in_schema=False)
def descarga_reporte(request: FechaReporteAsistenciaRequest):
    semaforo_service = SemaforoService()
    ruta_archivo =  semaforo_service.descargarReporte(request.fecha_inicio, request.fecha_fin)
   
    if not os.path.exists(ruta_archivo):
        raise HTTPException(status_code=404, detail="El archivo no fue encontrado")

    return FileResponse(
        ruta_archivo,
        filename=ruta_archivo.split("/")[-1],
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@router.get("/reporte", include_in_schema=False)
def generate():
    return {'message': 'semaforo'}