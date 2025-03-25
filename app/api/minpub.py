from fastapi import APIRouter, UploadFile, File, Form, BackgroundTasks
from datetime import datetime
from pathlib import Path
import os
from fastapi import HTTPException


from app.modules.sga.service_tecnico_operaciones import SGAService
from app.modules.sga.minpub.report_validator.service.transform_sga_335 import transform_sga_335
from app.modules.sga.minpub.report_validator.service.transform_excel_corte import transform_excel_corte
from app.modules.sga.minpub.report_validator.service.transform_word import transform_word


BASE_DIR = Path(__file__).resolve().parent.parent.parent
SAVE_DIR_EXTRACT_WORD_DATOS = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "word_datos"
SAVE_DIR_EXTRACT_WORD_TELEFONIA = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "word_telefonia"
SAVE_DIR_EXTRACT_EXCEL = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "excel"
SAVE_DIR_EXTRACT_SGA_335 = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "sga_335"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   


router = APIRouter(prefix="/api/minpub", tags=["minpub"])
 

async def save_file(uploaded_file: UploadFile, save_dir: str) -> str:
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = uploaded_file.filename.split(".")[0]
    extension = uploaded_file.filename.split(".")[1]

    filename_timestamp = f"{filename}_{timestamp}.{extension}"
    file_path = os.path.join(save_dir, filename_timestamp)
    with open(file_path, "wb") as buffer:
        buffer.write(await uploaded_file.read())
    return file_path


@router.post("/process/")
async def process_files(
    word_file_datos: UploadFile = File(...),
    word_file_telefonia: UploadFile = File(...),
    excel_file: UploadFile = File(...),
    fecha_inicio: str = Form(...),
    fecha_fin: str = Form(...),
):

    word_datos_file_path = await save_file(word_file_datos, SAVE_DIR_EXTRACT_WORD_DATOS)
    word_telefonia_file_path = await save_file(word_file_telefonia, SAVE_DIR_EXTRACT_WORD_TELEFONIA)
    excel_file_path = await save_file(excel_file, SAVE_DIR_EXTRACT_EXCEL)
    sga_file_path = save_file(fecha_inicio, fecha_fin)

    sga_service = SGAService()

    indice_tabla_reporte_data_previa = 13  # 334 INCIPENDIENTE_MINPUB 
    indice_tabla_reporte_detalle = 15  # INC_SLA_MINPUB 335 

    ruta_archivo = sga_service.generate_dynamic_report(fecha_inicio, fecha_fin, indice_tabla_reporte_data_previa , indice_tabla_reporte_detalle)

    if not os.path.exists(ruta_archivo):
        raise HTTPException(status_code=404, detail="El archivo no fue encontrado")


    df_word_datos_transformed = transform_word(word_datos_file_path)
    df_word_telefonia_transformed = transform_word(word_telefonia_file_path)
    df_excel_corte_transformed = transform_excel_corte(excel_file_path)
    df_sga_transformed = transform_sga_335(sga_file_path)
