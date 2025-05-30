from fastapi import APIRouter, HTTPException, UploadFile, File, Form, BackgroundTasks

from datetime import datetime
from pathlib import Path
import os
import asyncio
import uuid
from app.modules.sga.service_tecnico_operaciones import SGAService
from app.modules.sga.minpub.report_validator.service.objetivos.all_objetivos import all_objetivos
from app.modules.sga.minpub.report_validator.service.objetivos.etl.extract.corte_excel import extract_corte_excel

extract_corte_excel

import threading

BASE_DIR = Path(__file__).resolve().parent.parent.parent
SAVE_DIR_EXTRACT_WORD_DATOS = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "word_datos"
SAVE_DIR_EXTRACT_WORD_TELEFONIA = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "word_telefonia"
SAVE_DIR_EXTRACT_EXCEL = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "excel"
SAVE_DIR_EXTRACT_SGA_335 = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "sga_335" 
CID_CUISMP_PATH = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "sharepoint_cid_cuismp"
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 

router = APIRouter(prefix="/api/minpub", tags=["minpub"])


processing_tasks = {}
 
sga_lock = threading.Lock()

async def save_file(uploaded_file: UploadFile, save_dir: str) -> str:

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name, extension = os.path.splitext(uploaded_file.filename)
    file_name_timestamp = f"{base_name}_{timestamp}{extension}"
    file_path = os.path.join(save_dir, file_name_timestamp)

    with open(file_path, "wb") as buffer:
        buffer.write(await uploaded_file.read())
    return file_path


def locked_generate_dynamic_report(sga_service, fecha_inicio, fecha_fin, indice_data, indice_detalle):
    with sga_lock:
        print("[FastAPI] Automatización tomando control del mouse...")
        return sga_service.generate_dynamic_report(fecha_inicio, fecha_fin, indice_data, indice_detalle)

async def process_task(task_id:str, fecha_inicio, fecha_fin, word_datos_file_path, word_telefonia_file_path, excel_file_path, sharepoint_cid_cuismp_path):
      
    try:
        processing_tasks[task_id] = {"status": "processing", "result": None}
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
            processing_tasks[task_id] = {"status": "failed", "error": str(e)}
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
            processing_tasks[task_id] = {"status": "failed", "error": str(e)}
            return

        results = await asyncio.to_thread(
            all_objetivos,
            excel_file_path,
            sga_file_path_335,
            sga_file_path_380,
            sharepoint_cid_cuismp_path,
            word_datos_file_path,
            word_telefonia_file_path

            )
        processing_tasks[task_id] = {"status":"completed", "result": results}
        return results


    except Exception as e:
        processing_tasks[task_id] = {"status": "failed", "error": str(e)}
        print(f"Error processing task {task_id}: {e}")

@router.post("/process/")
async def process_files(
    background_tasks: BackgroundTasks,
    word_file_datos: UploadFile = File(...),
    word_file_telefonia: UploadFile = File(...),
    excel_file: UploadFile = File(...),
    excel_file_cuismp: UploadFile = File(...),
    fecha_inicio: str = Form(...),
    fecha_fin: str = Form(...),
):
    
    """Starts background processing and returns a task ID for polling."""

    word_datos_file_path = await save_file(word_file_datos, SAVE_DIR_EXTRACT_WORD_DATOS)
    word_telefonia_file_path = await save_file(word_file_telefonia, SAVE_DIR_EXTRACT_WORD_TELEFONIA)
    excel_file_path = await save_file(excel_file, SAVE_DIR_EXTRACT_EXCEL)
    excel_file_cuismp_path = await save_file(excel_file_cuismp, CID_CUISMP_PATH)
    
    try:
        extract_corte_excel(excel_file_path, skipfooter=2)
    except HTTPException as exc:
        raise exc

    task_id = str(uuid.uuid4()) 
    processing_tasks[task_id] = {"status":"queued", "result": None}

    background_tasks.add_task(
        process_task,
        task_id,
        fecha_inicio,
        fecha_fin,
        word_datos_file_path,
        word_telefonia_file_path,
        excel_file_path,
        excel_file_cuismp_path
        )

    return {"task_id": task_id, "status": "queued"}

@router.get("/status/{task_id}")
async def check_status(task_id: str):
    """
    Returns the status of the background processing task.
    If the task is completed, the result is also returned.
    """
    
    task_info = processing_tasks.get(task_id)

    if not task_info:
         return {"task_id": task_id, "status": "not_found"}
    return {"task_id": task_id,
            "status": task_info.get("status"),
            "result": task_info.get("result"),
            "error": task_info.get("error"),
            }    

   


