from fastapi import APIRouter, UploadFile, File, Form, BackgroundTasks

from datetime import datetime
from pathlib import Path
import os
import asyncio
import uuid
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


processing_tasks = {}
 

async def save_file(uploaded_file: UploadFile, save_dir: str) -> str:
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = uploaded_file.filename.split(".")[0]
    extension = uploaded_file.filename.split(".")[1]
    filename_timestamp = f"{filename}_{timestamp}.{extension}"
    file_path = os.path.join(save_dir, filename_timestamp)

    with open(file_path, "wb") as buffer:
        buffer.write(await uploaded_file.read())
    return file_path

async def process_task(task_id:str, fecha_inicio, fecha_fin):
      
    try:
        processing_tasks[task_id] = "processing"

        sga_service = SGAService()
        indice_tabla_reporte_data_previa = 13   # INCIPENDIENTE_MINPUB
        indice_tabla_reporte_detalle = 15   # INC_SLA_MINPUB

        sga_file_path = await asyncio.to_thread(
            sga_service.generate_dynamic_report, fecha_inicio, fecha_fin, indice_tabla_reporte_data_previa, indice_tabla_reporte_detalle
        )

        if not os.path.exists(sga_file_path):
            processing_tasks[task_id] = "failed"
            return

        processing_tasks[task_id] = "completed"

    except Exception as e:
        processing_tasks[task_id] = "failed"
        print(f"Error processing task {task_id}: {e}")

@router.post("/process/")
async def process_files(
    background_tasks: BackgroundTasks,
    word_file_datos: UploadFile = File(...),
    word_file_telefonia: UploadFile = File(...),
    excel_file: UploadFile = File(...),
    fecha_inicio: str = Form(...),
    fecha_fin: str = Form(...),
):
    
    """Starts background processing and returns a task ID for polling."""

    word_datos_file_path = await save_file(word_file_datos, SAVE_DIR_EXTRACT_WORD_DATOS)
    word_telefonia_file_path = await save_file(word_file_telefonia, SAVE_DIR_EXTRACT_WORD_TELEFONIA)
    excel_file_path = await save_file(excel_file, SAVE_DIR_EXTRACT_EXCEL)

    task_id = str(uuid.uuid4())  # Generate a unique task ID
    processing_tasks[task_id] = "queued"

    background_tasks.add_task(process_task, task_id, fecha_inicio, fecha_fin)

    return {"task_id": task_id, "status": "queued"}

@router.get("/status/{task_id}")
async def check_status(task_id: str):
    """Returns the status of a processing task."""
    status = processing_tasks.get(task_id, "not_found")
    return {"task_id": task_id, "status": status}

   


