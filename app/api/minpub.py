from fastapi import APIRouter, UploadFile, File, Form, BackgroundTasks
from datetime import datetime
from pathlib import Path
import os


BASE_DIR = Path(__file__).resolve().parent.parent.parent
SAVE_DIR_EXTRACT_WORD_DATOS = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "word_datos"
SAVE_DIR_EXTRACT_WORD_TELEFONIA = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "word_telefonia"
SAVE_DIR_EXTRACT_EXCEL = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "excel"


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
