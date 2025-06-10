from pydantic import ValidationError
from app.modules.sga.models import FechaSecuenciaRequest
from fastapi import APIRouter, HTTPException, WebSocket
import os
from fastapi.responses import FileResponse
from ..modules.sga.service_tecnico_operaciones import SGAService
from ..modules.sga.service_crm_clientes import send_alert
from app.shared.lock import global_lock  # usa el lock global

from app.tasks.automation_tasks import process_sga_report_task
from pathlib import Path
from app.api.websocket import websocket_endpoint

BASE_DIR = Path(__file__).resolve().parent.parent.parent
SAVE_DIR_SGA_REPORTS = BASE_DIR / "media" / "sga" / "reports"

router = APIRouter(prefix="/api/sga", tags=["sga"])

@router.post("/reporte-descarga-averias-gestionado")
def descarga_reporte(request: FechaSecuenciaRequest):
    """Starts Celery task for generating and downloading the averias gestionado report."""
    
    # Start Celery task
    task = process_sga_report_task.delay(
        fecha_inicio=request.fecha_inicio,
        fecha_fin=request.fecha_fin,
        indice_tabla_reporte_data_previa=18,  # TABLERO 275 DATA PREVIA 
        indice_tabla_reporte_detalle=4,   # TABLERO 276 AVERIAS Y GESTIONADOS
        report_type="averias_gestionado"
    )

    return {"task_id": task.id, "status": "queued"}

@router.post("/reporte-descarga-minpub", include_in_schema=False)
def descarga_reporte(request: FechaSecuenciaRequest):
    """Starts Celery task for generating and downloading the minpub report."""
    
    # Start Celery task
    task = process_sga_report_task.delay(
        fecha_inicio=request.fecha_inicio,
        fecha_fin=request.fecha_fin,
        indice_tabla_reporte_data_previa=13,  # INCIPENDIENTE_MINPUB
        indice_tabla_reporte_detalle=15,  # INC_SLA_MINPUB 335
        report_type="minpub"
    )

    return {"task_id": task.id, "status": "queued"}

@router.get("/status/{task_id}")
async def check_status(task_id: str):
    """Returns the status of the Celery task."""
    task = process_sga_report_task.AsyncResult(task_id)
    
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
            'result': task.result
        }
    else:
        response = {
            'task_id': task_id,
            'status': 'failed',
            'error': str(task.info)
        }
    
    return response

@router.get("/download/{task_id}")
async def download_report(task_id: str):
    """Downloads the generated report file."""
    task = process_sga_report_task.AsyncResult(task_id)
    
    if task.state != 'SUCCESS':
        raise HTTPException(status_code=400, detail="Task not completed yet")
    
    file_path = task.result.get('file_path')
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(
        file_path,
        filename=os.path.basename(file_path),
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@router.post("/reporte-envio-alerta", include_in_schema=False)
def generate_send_alert():
    return send_alert()

@router.websocket("/ws/task/{task_id}")
async def websocket_task_status(websocket: WebSocket, task_id: str):
    await websocket_endpoint(websocket, task_id)


