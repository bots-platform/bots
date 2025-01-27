from pydantic import ValidationError
from app.modules.sga.models import FechaSecuenciaRequest
from fastapi import APIRouter, HTTPException
from ..modules.sga.service_tecnico_operaciones import SGAService
from ..modules.sga.service_CRM_clientes import send_alert

router = APIRouter(prefix="/api/sga", tags=["sga"])

@router.post("/reporte")
async def generate_dynamic_report(request: FechaSecuenciaRequest):
    sga_service = SGAService()
    return await sga_service.generate_dynamic_report(request.fecha_inicio, request.fecha_fin)
      
@router.post("/reporteEnvioAlerta")
def generate_send_alert():
    return send_alert()

@router.get("/reporte")
async def generate():
    return {'hola': 'sga - tickets'}



