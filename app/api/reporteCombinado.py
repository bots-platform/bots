
import os
from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse
from app.modules.web_bots.reportesCombinados.models import FechaReporteCombinadoRequest
from ..modules.web_bots.reportesCombinados.reporte_combinado_scripts import generar_reporte_combinado

router = APIRouter(prefix="/api/reportes", tags=["Reportes Combinados"])

@router.post("/combinado", include_in_schema=False)
def generar_reporte_combinado_endpoint(request: FechaReporteCombinadoRequest):

    """
    Genera un reporte combinado en Excel basado en un rango de fechas proporcionado y permite su descarga.

    - **fecha_inicio**: La fecha de inicio del rango (formato YYYY-MM-DD).
    - **fecha_fin**: La fecha de fin del rango (formato YYYY-MM-DD).
    """
    
    ruta_archivo = generar_reporte_combinado(request.fecha_inicio, request.fecha_fin)

    if not os.path.exists(ruta_archivo):
        raise HTTPException(status_code=404, detail="El archivo no fue encontrado")

    return FileResponse(
        ruta_archivo,
        filename=ruta_archivo.split("/")[-1],
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

@router.get("/combinado", include_in_schema=False)
def generate():
    return {'message': 'reporteCombinado'}


