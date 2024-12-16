from fastapi import APIRouter
from app.modules.web_bots.sharepoint.service import SharepointService

router = APIRouter(prefix="/api/sharepoint", tags=["sharepoint"])

@router.post("/reporte")
def descarga_reporte():
    sharepoint_service = SharepointService()
    respone =  sharepoint_service.descargarReporte()
    print(respone)
    return respone

@router.get("/reporte")
def generate():
    return {'hola': 'sharepoint'}