from fastapi import APIRouter

from ....modules.sga.service import SGAService

router = APIRouter(prefix="/api/sga", tags=["sga"])

@router.post("/reporte")
async def generate_dynamic_report():
    sga_service = SGAService()
    return await sga_service.generate_dynamic_report()



@router.get("/reporte")
async def generate():
    return {'hola': 'sga'}



