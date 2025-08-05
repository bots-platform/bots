from fastapi import APIRouter, UploadFile, File, Form, WebSocket
from datetime import datetime
from pathlib import Path
import os
from app.modules.sga.minpub.report_validator.service.objetivos.all_objetivos import all_objetivos
from app.tasks.automation_tasks import process_minpub_task
from app.api.websocket import websocket_endpoint

BASE_DIR = Path(__file__).resolve().parent.parent.parent
SAVE_DIR_EXTRACT_WORD_DATOS = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "word_datos"
SAVE_DIR_EXTRACT_WORD_TELEFONIA = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "word_telefonia"
SAVE_DIR_EXTRACT_EXCEL = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "excel"
SAVE_DIR_EXTRACT_SGA_335 = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "sga_335" 
SAVE_DIR_EXTRACT_SGA_380 = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "pausa_cliente"
CID_CUISMP_PATH = BASE_DIR / "media" / "minpub" / "validator_report" / "extract" / "sharepoint_cid_cuismp"

router = APIRouter(prefix="/api/minpub", tags=["minpub"])

async def save_file(uploaded_file: UploadFile, save_dir: str) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name, extension = os.path.splitext(uploaded_file.filename)
    file_name_timestamp = f"{base_name}_{timestamp}{extension}"
    file_path = os.path.join(save_dir, file_name_timestamp)

    with open(file_path, "wb") as buffer:
        buffer.write(await uploaded_file.read())
    return file_path

@router.post("/process/")
async def process_files(
    word_file_datos: UploadFile = File(...),
    word_file_telefonia: UploadFile = File(...),
    excel_file: UploadFile = File(...),
    excel_file_cuismp: UploadFile = File(...),
    fecha_inicio: str = Form(...),
    fecha_fin: str = Form(...),
):
  
    word_datos_file_path = await save_file(word_file_datos, SAVE_DIR_EXTRACT_WORD_DATOS)
    word_telefonia_file_path = await save_file(word_file_telefonia, SAVE_DIR_EXTRACT_WORD_TELEFONIA)
    excel_file_path = await save_file(excel_file, SAVE_DIR_EXTRACT_EXCEL)
    excel_file_cuismp_path = await save_file(excel_file_cuismp, CID_CUISMP_PATH)

    task = process_minpub_task.delay(
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        word_datos_file_path=word_datos_file_path,
        word_telefonia_file_path=word_telefonia_file_path,
        excel_file_path=excel_file_path,
        sharepoint_cid_cuismp_path=excel_file_cuismp_path
    )

    return {"task_id": task.id, "status": "queued"}

@router.get("/status/{task_id}")
async def check_status(task_id: str):
    """Returns the status of the Celery task."""
    task = process_minpub_task.AsyncResult(task_id)
    
    if task.state == 'PENDING':
        response = {
            'task_id': task_id,
            'status': 'pending',
            'info': None
        }
    elif task.state == 'PROGRESS':
        response = {
            'task_id': task_id,
            'status': 'processing',
            'info': task.info
        }
    elif task.state == 'SUCCESS':
        response = {
            'task_id': task_id,
            'status': 'completed',
            'result': task.result["result"]
        }
    else:
        response = {
            'task_id': task_id,
            'status': 'failed',
            'error': str(task.info)
        }
    
    return response

@router.websocket("/ws/task/{task_id}")
async def websocket_task_status(websocket: WebSocket, task_id: str):
    await websocket_endpoint(websocket, task_id)

@router.post("/process-manual/")
async def process_v2(
    word_file_datos: UploadFile = File(...),
    word_file_telefonia: UploadFile = File(...),
    excel_file: UploadFile = File(...),
    excel_file_cuismp: UploadFile = File(...),
    excel_sga_minpub: UploadFile = File(...),        
    excel_sga_pausa_cliente: UploadFile = File(...), 

):
   
    word_datos_file_path = await save_file(word_file_datos, SAVE_DIR_EXTRACT_WORD_DATOS)
    word_telefonia_file_path = await save_file(word_file_telefonia, SAVE_DIR_EXTRACT_WORD_TELEFONIA)
    excel_file_path = await save_file(excel_file, SAVE_DIR_EXTRACT_EXCEL)
    excel_file_cuismp_path = await save_file(excel_file_cuismp, CID_CUISMP_PATH)
    excel_sga_minpub_path = await save_file(excel_sga_minpub, SAVE_DIR_EXTRACT_SGA_335)
    excel_sga_pausa_cliente_path = await save_file(excel_sga_pausa_cliente, SAVE_DIR_EXTRACT_SGA_380)

 
    result = all_objetivos(
        excel_file_path,
        excel_sga_minpub_path,      
        excel_sga_pausa_cliente_path,  
        excel_file_cuismp_path,
        word_datos_file_path,
        word_telefonia_file_path
    )

    has_observations = len(result) > 0
    message = "✅ No se encontraron observaciones." if not has_observations else f"Se encontraron {len(result)} observaciones que requieren atención."

    return {
        "status": "completed", 
        "result": result,
        "has_observations": has_observations,
        "message": message,
        "count": len(result)
    }

   


