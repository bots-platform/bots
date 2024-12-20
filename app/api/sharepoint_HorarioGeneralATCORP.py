from fastapi import APIRouter
from fastapi.responses import FileResponse
from app.modules.web_bots.sharepoint.service import SharepointService
from app.modules.web_bots.sharepoint.scripts.horario_General_ATCORP import (
    guardar_excel_como
)

router = APIRouter(prefix="/api/sharepoint", tags=["Horario-General-ATCORP_2024"])

@router.post("/reporteHorarioGeneralATCORP")
def descarga_reporte():
    ruta_archivo =  guardar_excel_como()
    print(ruta_archivo)
    return FileResponse(
        ruta_archivo,
        filename=ruta_archivo.split("/")[-1],
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )



# @router.get("/reporte")
# def generate():
#     return {'hola': 'sharepoint'}



