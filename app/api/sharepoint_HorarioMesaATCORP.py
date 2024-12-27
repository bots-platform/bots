from fastapi import APIRouter
from fastapi.responses import FileResponse
from app.modules.web_bots.sharepoint.service import SharepointService
from app.modules.web_bots.sharepoint.scripts.horario_Mesa_ATCORP import (
    save_from_Sync_Desktop_Excel
)

router = APIRouter(prefix="/api/sharepoint", tags=["Horario-Mesa-ATCORP_2024"])

@router.post("/reporteHorarioMesaATCORP")
def descarga_reporte():
    ruta_archivo =  save_from_Sync_Desktop_Excel()
    print(ruta_archivo)
    return FileResponse(
        ruta_archivo,
        filename=ruta_archivo.split("/")[-1],
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

# @router.get("/reporte")
# def generate():
#     return {'hola': 'sharepoint'}



