from fastapi import APIRouter, UploadFile, File, Form, BackgroundTasks

from datetime import datetime
from pathlib import Path
import os
import asyncio
import uuid
from app.modules.sga.service_tecnico_operaciones import SGAService
from app.modules.sga.minpub.report_validator.service.objetivos.objetivo1 import objetivo_1
from app.modules.sga.minpub.report_validator.service.objetivos.objetivo2 import objetivo_2
from app.modules.sga.minpub.report_validator.service.objetivos.objetivo3 import objetivo_3
from app.modules.sga.minpub.report_validator.service.objetivos.objetivo4 import objetivo_4
from app.modules.sga.minpub.report_validator.service.objetivos.objetivo5 import objetivo_5

import threading

BASE_DIR = Path(__file__).resolve().parent.parent.parent
SAVE_DIR_EXTRACT_WORD_DATOS = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "word_datos"
SAVE_DIR_EXTRACT_WORD_TELEFONIA = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "word_telefonia"
SAVE_DIR_EXTRACT_EXCEL = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "excel"
SAVE_DIR_EXTRACT_SGA_335 = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "sga_335" 
CID_CUISMP_PATH = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "sharepoint_cid_cuismp" / "MINPU - CID-CUISMP - AB.xlsx"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 

router = APIRouter(prefix="/api/minpub", tags=["minpub"])


processing_tasks = {}
 
sga_lock = threading.Lock()

async def save_file(uploaded_file: UploadFile, save_dir: str) -> str:
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = uploaded_file.filename.split(".")[0]
    extension = uploaded_file.filename.split(".")[1]
    filename_timestamp = f"{filename}_{timestamp}.{extension}"
    file_path = os.path.join(save_dir, filename_timestamp)

    with open(file_path, "wb") as buffer:
        buffer.write(await uploaded_file.read())
    return file_path


def locked_generate_dynamic_report(sga_service, fecha_inicio, fecha_fin, indice_data, indice_detalle):
    with sga_lock:
        return sga_service.generate_dynamic_report(fecha_inicio, fecha_fin, indice_data, indice_detalle)
    

async def process_task(task_id:str, fecha_inicio, fecha_fin, word_datos_file_path, word_telefonia_file_path, excel_file_path, sharepoint_cid_cuismp_path):
      
    try:
        processing_tasks[task_id] = "processing"
        sga_service = SGAService()

        indice_tabla_reporte_data_previa = 13   # DATA PREVIA MINPUB 333
        indice_tabla_reporte_detalle = 15   # INC_SLA_MINPUB  335
        sga_file_path_335 = await asyncio.to_thread(
            locked_generate_dynamic_report,
            sga_service,
            fecha_inicio,
            fecha_fin,
            indice_tabla_reporte_data_previa,
            indice_tabla_reporte_detalle
        )

        if not os.path.exists(sga_file_path_335):
            processing_tasks[task_id] = "failed"
            return
        
        await asyncio.sleep(7)
        
        indice_tabla_reporte_data_previa = 13  # DATA PREVIA MINPUB 333
        indice_tabla_reporte_detalle = 18   # PARADA_RELOJ_INCIDENCIA 380
        sga_file_path_380 = await asyncio.to_thread(
            locked_generate_dynamic_report,
            sga_service,
            fecha_inicio,
            fecha_fin,
            indice_tabla_reporte_data_previa,
            indice_tabla_reporte_detalle
        )

        if not os.path.exists(sga_file_path_380):
            processing_tasks[task_id] = "failed"
            return

        df_objetivo_1 = await asyncio.to_thread(objetivo_1, excel_file_path, sga_file_path_335, sga_file_path_380, sharepoint_cid_cuismp_path)

        

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
    sharepoint_cid_cuismp_path = CID_CUISMP_PATH
    

    task_id = str(uuid.uuid4()) 
    processing_tasks[task_id] = "queued"

    background_tasks.add_task(process_task, task_id, fecha_inicio, fecha_fin, word_datos_file_path, word_telefonia_file_path, excel_file_path, sharepoint_cid_cuismp_path)

    return {"task_id": task_id, "status": "queued"}

@router.get("/status/{task_id}")
async def check_status(task_id: str):
    """Returns the status of a processing task."""
    status = processing_tasks.get(task_id, "not_found")
    return {"task_id": task_id, "status": status}

   


