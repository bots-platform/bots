from app.modules.sga.models import FechaSecuenciaRequest
from fastapi import APIRouter
from ..modules.sga.service import SGAService

router = APIRouter(prefix="/api/sga", tags=["sga"])

@router.post("/reporte")
async def generate_dynamic_report(request: FechaSecuenciaRequest):
    sga_service = SGAService()
    return await sga_service.generate_dynamic_report(request.fecha_inicio, request.fecha_fin)

@router.get("/reporte")
async def generate():
    return {'hola': 'sga - tickets'}



