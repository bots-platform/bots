from fastapi import HTTPException
from fastapi import APIRouter, UploadFile, File
from fastapi.responses import FileResponse
from typing import Optional
from pydantic import BaseModel
import os
from datetime import datetime
import shutil
import pandas as pd

from app.modules.words_bots.pronatel.scripts.pronatel import create_reportes_by_ticket_by_client, validate_required_columns_from_excel  

router = APIRouter(prefix="/api/reportes", tags=["reportes_tickets"])

class ProcessExcelResponse(BaseModel):
    message : str
    excel_id: str
    filename : str

class GenerateWordRequest(BaseModel):
    excel_id: str
    # fecha_inicio: Optional[str] = None
    # fecha_fin: Optional[str] = None

class Config:
    json_schema_extra = {
        "example":{
            "message": "Archivo excel procesado correctamente",
            "excel_id": "excel_20240131_123456",
            "file_name": "data.xlsx"
        }
    }


BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # Obtiene el directorio del script actual

UPLOAD_DIR = os.path.join(BASE_DIR, "..", "..", "media", "pronatel", "data")
WORD_OUTPUT_DIR = os.path.join(BASE_DIR, "..", "..", "media", "pronatel", "reportes")
WORD_PLANTILLA_PATH = os.path.join(BASE_DIR, "..", "..", "media", "pronatel", "plantillas")

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(WORD_OUTPUT_DIR, exist_ok=True)
os.makedirs(WORD_PLANTILLA_PATH, exist_ok=True)


@router.post("/upload-excel", response_model=ProcessExcelResponse)
async def upload_excel_file(file: UploadFile = File(...)):
    """
    Endpoint para subir el archivo excel con los datos de los tickets.

    Args:
    file: Archivo excel a subir

    Returns:
        ProcessExcelResponse: Informacion sobre el archivo procesado

    """

    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_id = f"excel_{timestamp}"

        file_extension= os.path.splitext(file.filename)[1]
        filename = f"{excel_id}{file_extension}"
        file_path = os.path.join(UPLOAD_DIR, filename)

        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        return ProcessExcelResponse(
            message="Archivo excel procesado correctamente",
            excel_id=excel_id,
            filename=filename 
        )
    
    except Exception as e:
        raise HTTPException(
        status_code=500,
        detail=f"Error al procesar el archivo: {str(e)}"
        )

@router.post("/generate-word")
async def generate_word_report(request: GenerateWordRequest):
    """
    Endpoint para generar el reporte word basado en el Excel subido.

    Args:

    request: Datos para la generacion del reporte

    Returns:
        FileResponse: Archivo Word generado
    
    """

    try:
        excel_path = os.path.join(UPLOAD_DIR, f"{request.excel_id}.xlsx")
        word_template_path = os.path.join(WORD_PLANTILLA_PATH, "plantilla_pronatel_logo.docx")
      
        if not os.path.exists(excel_path):
           raise HTTPException(404, "Archivo Excel no encontrado")

        df_validate =  validate_required_columns_from_excel(excel_path)
        
        try:
           word_output_path  = create_reportes_by_ticket_by_client(
               df=df_validate,
               word_template_path=word_template_path,
               word_output_path=WORD_OUTPUT_DIR,

           )

        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error inesperado : {str(e)}")

        if not os.path.exists(word_output_path):
            raise HTTPException(status_code=500, detail="Error: No se guardo el archivo Word.")

        return FileResponse(
            word_output_path,
            filename=os.path.basename(word_output_path), 
            media_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        )
    

    except HTTPException:
        raise
    except Exception as e:
       raise HTTPException( status_code=500, detail=str(e))
    

    

        
    

    

