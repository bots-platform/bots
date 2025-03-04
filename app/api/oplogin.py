from fastapi import APIRouter
from app.modules.web_bots.oplogin.service import OploginService

router = APIRouter(prefix="/api/oplogin", tags=["oplogin"])

@router.post("/reporte", include_in_schema=False)
def descarga_reporte():
    oplogin_service = OploginService()
    respone =  oplogin_service.descargarReporte()
    return respone

@router.get("/reporte", include_in_schema=False)
def generate():
    return {'message': 'oplogin'}