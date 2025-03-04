from fastapi import APIRouter
import os
from fastapi import  HTTPException
from fastapi.responses import FileResponse
from app.modules.web_bots.sharepoint.scripts.horario_mesa_atcorp import (
    save_from_Sync_Desktop_Excel
)

router = APIRouter(prefix="/api/sharepoint", tags=["Horario Mesa ATCORP"])

@router.post("/reporte-horario-mesa-atcorp", include_in_schema=False)
def descarga_reporte():
    ruta_archivo =  save_from_Sync_Desktop_Excel()
    
    if not os.path.exists(ruta_archivo):
        raise HTTPException(status_code=404, detail="El archivo no fue encontrado")

    return FileResponse(
        ruta_archivo,
        filename=ruta_archivo.split("/")[-1],
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@router.get("/reporte", include_in_schema=False)
def generate():
    return {'message': 'sharepoint'}



