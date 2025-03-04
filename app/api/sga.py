from pydantic import ValidationError
from app.modules.sga.models import FechaSecuenciaRequest
from fastapi import APIRouter, HTTPException
import os
from fastapi.responses import FileResponse
from ..modules.sga.service_tecnico_operaciones import SGAService
from ..modules.sga.service_crm_clientes import send_alert

router = APIRouter(prefix="/api/sga", tags=["sga"])

@router.post("/reporte-descarga")
def descarga_reporte(request: FechaSecuenciaRequest):
    sga_service = SGAService()
    ruta_archivo = sga_service.generate_dynamic_report(request.fecha_inicio, request.fecha_fin)

    if not os.path.exists(ruta_archivo):
        raise HTTPException(status_code=404, detail="El archivo no fue encontrado")

    return FileResponse(
        ruta_archivo,
        filename=ruta_archivo.split("/")[-1],
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
     
@router.post("/reporte-envio-alerta", include_in_schema=False)
def generate_send_alert():
    return send_alert()


