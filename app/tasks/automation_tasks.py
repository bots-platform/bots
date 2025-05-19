import time
from app.core.celery_app import celery_app
from app.modules.sga.service_tecnico_operaciones import SGAService
from app.modules.sga.minpub.report_validator.service.objetivos.all_objetivos import all_objetivos
import threading
from typing import Dict, Any
import os

# Global lock for SGA application to prevent concurrent access
sga_global_lock = threading.Lock()

# Thread locks for automation tools
selenium_lock = threading.Lock()

def wait_for_sga_service(sga_service: SGAService, max_wait_time: int = 300) -> bool:
    """
    Wait for SGA service to be available, checking every 5 seconds.
    Returns True if service became available, False if timeout reached.
    """
    start_time = time.time()
    while time.time() - start_time < max_wait_time:
        if sga_service.is_available():
            return True
        time.sleep(5)
    return False

@celery_app.task(queue="ui", bind=True, bind=True, ack_late=False, name="process_sga_report")
def process_sga_report_task(self, 
                          fecha_inicio: str,
                          fecha_fin: str,
                          indice_tabla_reporte_data_previa: int,
                          indice_tabla_reporte_detalle: int,
                          report_type: str) -> Dict[str, Any]:
    try:
        self.update_state(state='PROGRESS', meta={'status': 'Processing SGA report'})
        sga_service = SGAService()

        # Use global SGA lock to ensure only one process can access SGA at a time
        with sga_global_lock:
            if not wait_for_sga_service(sga_service):
                raise Exception("Timeout waiting for SGA service to be available")
            
            # Generate SGA report
            file_path = sga_service.generate_dynamic_report(
                fecha_inicio,
                fecha_fin,
                indice_tabla_reporte_data_previa,
                indice_tabla_reporte_detalle
            )

            if not os.path.exists(file_path):
                raise Exception("Generated file not found")

            return {
                "status": "completed",
                "file_path": file_path,
                "report_type": report_type
            }

    except Exception as e:
        return {"status": "failed", "error": str(e)}

@celery_app.task(queue="ui", bind=True, ack_late=False, task_reject_on_worker_lost=True, name="process_minpub")
def process_minpub_task(self, 
                       fecha_inicio: str,
                       fecha_fin: str,
                       word_datos_file_path: str,
                       word_telefonia_file_path: str,
                       excel_file_path: str,
                       sharepoint_cid_cuismp_path: str) -> Dict[str, Any]:
    try:
        self.update_state(state='PROGRESS', meta={'status': 'Processing SGA data'})
        sga_service = SGAService()
        sga_file_path_335 = None
        sga_file_path_380 = None

        # Generate SGA 335 report
        with sga_global_lock:
            if not wait_for_sga_service(sga_service):
                raise Exception("Timeout waiting for SGA service to be available for report 335")
            
            indice_tabla_reporte_data_previa = 13
            indice_tabla_reporte_detalle = 15
            sga_file_path_335 = sga_service.generate_dynamic_report(
                fecha_inicio,
                fecha_fin,
                indice_tabla_reporte_data_previa,
                indice_tabla_reporte_detalle
            )

            if not os.path.exists(sga_file_path_335):
                raise Exception("Generated file for report 335 not found")

        # Generate SGA 380 report
        with sga_global_lock:
            if not wait_for_sga_service(sga_service):
                raise Exception("Timeout waiting for SGA service to be available for report 380")
            
            indice_tabla_reporte_data_previa = 13
            indice_tabla_reporte_detalle = 18
            sga_file_path_380 = sga_service.generate_dynamic_report(
                fecha_inicio,
                fecha_fin,
                indice_tabla_reporte_data_previa,
                indice_tabla_reporte_detalle
            )

            if not os.path.exists(sga_file_path_380):
                raise Exception("Generated file for report 380 not found")

        self.update_state(state='PROGRESS', meta={'status': 'Processing objectives'})
        results = all_objetivos(
            excel_file_path,
            sga_file_path_335,
            sga_file_path_380,
            sharepoint_cid_cuismp_path,
            word_datos_file_path,
            word_telefonia_file_path
        )

        return {"status": "completed", "result": results}

    except Exception as e:
        return {"status": "failed", "error": str(e)}

@celery_app.task(bind=True, name="process_semaforo")
def process_semaforo_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
    try:
        with sga_global_lock:
            # Your Semaforo Selenium automation code here
            pass
    except Exception as e:
        return {"status": "failed", "error": str(e)}

@celery_app.task(bind=True, name="process_newcallcenter")
def process_newcallcenter_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
    try:
        with sga_global_lock:
            # Your NewCallCenter Selenium automation code here
            pass
    except Exception as e:
        return {"status": "failed", "error": str(e)}

@celery_app.task(bind=True, name="process_oplogin")
def process_oplogin_task(self, params: Dict[str, Any]) -> Dict[str, Any]:
    try:
        with sga_global_lock:
            # Your OPLogin Selenium automation code here
            pass
    except Exception as e:
        return {"status": "failed", "error": str(e)} 