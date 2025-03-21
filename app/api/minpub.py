from fastapi import APIRouter, UploadFile, File, Form, BackgroundTasks
import os

from ..modules.sga.minpub.report_validator.service.extract import extract_data
from ..modules.sga.minpub.report_validator.service.transform import transform_data
from ..modules.sga.minpub.report_validator.service.compare import compare_data

router = APIRouter(prefix="/api/validator", tags=["Validador de Reportes MINPUB"])

BASE_DIR = os.path.join(os.path.abspath(__file__))

SAVE_DIR_EXTRACT = os.path.join(BASE_DIR, "..", "..", "media", "minpub", "validator_report", "extract")    
SAVE_DIR_TRANSFORMED = os.path.join(BASE_DIR, "..", "..", "media", "minpub", "validator_report", "transformed")

os.makedirs(SAVE_DIR_EXTRACT, exist_ok=True)
os.makedirs(SAVE_DIR_TRANSFORMED, exist_ok=True)

@router.post("/process")
async def process_files(
        file1 : UploadFile = File(...),
        file2 : UploadFile = File(...),
        word_file: UploadFile = File(...),
        fecha_inicio: str = Form(...),
        fecha_fin: str = Form(...),
        background_tasks: BackgroundTasks = BackgroundTasks(),
):
    file_paths = {
        "file1": os.path.join(SAVE_DIR_EXTRACT, file1.filename),
        "file2": os.path.join(SAVE_DIR_EXTRACT, file2.filename),
        "word_file": os.path.join(SAVE_DIR_EXTRACT, word_file.filename)
    }

    for file_key, file_path in file_paths.items():
        with open(file_path, "wb") as buffer:
            buffer.write(await eval(file_key).read())
    
    extracted_data = extract_data(file_paths)

    background_tasks.add_task(transform_data, extracted_data, fecha_inicio, fecha_fin)

    return {"message": "Processing started", "files": file_paths}




