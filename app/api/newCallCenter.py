import os
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from app.modules.web_bots.newCallCenter.models import FechaReporteActividadAgenteRequest
from app.modules.web_bots.newCallCenter.service import NewCallCenterService

router = APIRouter(prefix="/api/newcallcenter", tags=["newcallcenter"])

@router.post("/reporte", include_in_schema=False)
def descarga_reporte(request: FechaReporteActividadAgenteRequest):
    NewCallCenter_service = NewCallCenterService()
    
    ruta_archivo =  NewCallCenter_service.descargarReporte(request.fecha_inicio, request.fecha_fin)

    if not os.path.exists(ruta_archivo):
        raise HTTPException(status_code=404, detail="El archivo no fue encontrado")
    
    return FileResponse(
        ruta_archivo,
        filename=ruta_archivo.split("/")[-1],
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

