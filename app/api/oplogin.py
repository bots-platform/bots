from fastapi import APIRouter

from app.modules.oplogin.service import OploginService

router = APIRouter(prefix="/api/oplogin", tags=["oplogin"])

@router.post("/reporte")
def descarga_reporte():
    oplogin_service = OploginService()
    respone =  oplogin_service.descargarReporte()
    print(respone)
    return respone

@router.get("/reporte")
def generate():
    return {'hola': 'oplogin'}