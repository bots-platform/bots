from fastapi import APIRouter, HTTPException
import os
from fastapi.responses import FileResponse
from app.modules.web_bots.sharepoint.scripts.horario_general_atcorp import (
    guardar_excel_como
)

router = APIRouter(prefix="/api/sharepoint", tags=["Horario General ATCORP"])

@router.post("/reporte-horario-general-atcorp", include_in_schema=False)
def descarga_reporte():
    ruta_archivo =  guardar_excel_como()

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



