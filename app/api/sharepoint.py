from fastapi import APIRouter
from fastapi.responses import FileResponse
from app.modules.web_bots.sharepoint.service import SharepointService

router = APIRouter(prefix="/api/Horario-General-ATcorp_2024", tags=["Horario-General-ATcorp_2024"])

@router.post("/reporte")
def descarga_reporte():
    sharepoint_service = SharepointService()
    ruta_archivo =  sharepoint_service.guardar_excel_como()
    print(ruta_archivo)
    return FileResponse(
        ruta_archivo,
        filename=ruta_archivo.split("/")[-1],
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

# @router.get("/reporte")
# def generate():
#     return {'hola': 'sharepoint'}



