import time
from app.core.celery_app import celery_app
from app.modules.sga.service_tecnico_operaciones import SGAService
from app.modules.sga.minpub.report_validator.service.objetivos.all_objetivos import all_objetivos
import threading
from app.shared.lock import global_lock
from app.shared.activity_monitor import get_activity_status, reset_activity_status
from typing import Dict, Any
import os
import random
#from pywinauto.mouse import click
import logging
from celery import shared_task
from celery.utils.log import get_task_logger

logger = logging.getLogger(__name__)


sga_global_lock = global_lock


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

@celery_app.task(queue="ui", bind=True, ack_late=False, task_reject_on_worker_lost=True, name="process_sga_report")
def process_sga_report_task(self, 
                          fecha_inicio: str,
                          fecha_fin: str,
                          indice_tabla_reporte_data_previa: int,
                          indice_tabla_reporte_detalle: int,
                          report_type: str) -> Dict[str, Any]:
    try:
     
        self.update_state(state='PROGRESS', meta={'message': 'Preparando generación de reporte...', 'progress': 0})
        sga_service = SGAService()

        self.update_state(state='PROGRESS', meta={'message': 'Esperando acceso a SGA...', 'progress': 5})
        with sga_global_lock:
            if not wait_for_sga_service(sga_service):
                raise Exception("Timeout esperando SGA para generar reporte")

            self.update_state(state='PROGRESS', meta={'message': 'Generando reporte SGA (esto puede tardar varios minutos)...', 'progress': 10})
            file_path = sga_service.generate_dynamic_report(
                fecha_inicio,
                fecha_fin,
                indice_tabla_reporte_data_previa,
                indice_tabla_reporte_detalle
            )

            self.update_state(state='PROGRESS', meta={'message': 'Validando archivo generado...', 'progress': 95})
            if not os.path.exists(file_path):
                raise Exception("No se encontró el archivo generado")

        self.update_state(state='PROGRESS', meta={'message': 'Reporte generado exitosamente', 'progress': 100})
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
        self.update_state(state='PROGRESS', meta={'message': 'Archivos recibidos, preparando procesamiento...', 'progress': 0})

        sga_service = SGAService()
        sga_file_path_335 = None
        sga_file_path_380 = None

        self.update_state(state='PROGRESS', meta={'message': 'Generando reporte SGA 335 (esto puede tardar varios minutos)...', 'progress': 5})
        with sga_global_lock:
            if not wait_for_sga_service(sga_service):
                raise Exception("Timeout esperando SGA para reporte 335")
            indice_tabla_reporte_data_previa = 13
            indice_tabla_reporte_detalle = 15
            sga_file_path_335 = sga_service.generate_dynamic_report(
                fecha_inicio,
                fecha_fin,
                indice_tabla_reporte_data_previa,
                indice_tabla_reporte_detalle
            )
            if not os.path.exists(sga_file_path_335):
                raise Exception("No se encontró el archivo generado para reporte 335")
        self.update_state(state='PROGRESS', meta={'message': 'Reporte SGA 335 generado', 'progress': 55})

    
        self.update_state(state='PROGRESS', meta={'message': 'Generando reporte SGA 380 (esto puede tardar varios minutos)...', 'progress': 55})
        with sga_global_lock:
            if not wait_for_sga_service(sga_service):
                raise Exception("Timeout esperando SGA para reporte 380")
            indice_tabla_reporte_data_previa = 13
            indice_tabla_reporte_detalle = 18
            sga_file_path_380 = sga_service.generate_dynamic_report(
                fecha_inicio,
                fecha_fin,
                indice_tabla_reporte_data_previa,
                indice_tabla_reporte_detalle
            )
            if not os.path.exists(sga_file_path_380):
                raise Exception("No se encontró el archivo generado para reporte 380")
        self.update_state(state='PROGRESS', meta={'message': 'Reporte SGA 380 generado', 'progress': 95})

        self.update_state(state='PROGRESS', meta={'message': 'Procesando objetivos y generando resultados...', 'progress': 95})
        results = all_objetivos(
            excel_file_path,
            sga_file_path_335,
            sga_file_path_380,
            sharepoint_cid_cuismp_path,
            word_datos_file_path,
            word_telefonia_file_path
        )

        self.update_state(state='PROGRESS', meta={'message': 'Procesamiento finalizado', 'progress': 100})

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

@celery_app.task(queue="ui", bind=True, name="keep_system_active")
def keep_system_active_task(self):
    """
    Tarea que mantiene el sistema activo simulando actividad del mouse.
    Se ejecuta cada 25-40 segundos si no hay actividad humana.
    """
    logger.info("Ejecutando keep_system_active_task...")
    patrones = ["click_1", "click_2"]
    
    # Obtener estado de actividad desde archivo temporal
    actividad_humana = get_activity_status()
    
    if actividad_humana["mouse"] or actividad_humana["teclado"]:
        logger.info("[keep_system_active] Usuario está activo, no simulo nada.")
    else:
        logger.info("[keep_system_active] Intentando adquirir lock...")
        if sga_global_lock.acquire(blocking=False):
            try:
                accion = random.choice(patrones)
                if accion == "click_1":
                    logger.info("[keep_system_active] Realizando clic en (35,10)")
                    click(button='left', coords=(135, 10))
                elif accion == "click_2":
                    logger.info("[keep_system_active] Realizando clic en (115,5)")
                    click(button='left', coords=(115, 5))
            finally:
                logger.info("[keep_system_active] Liberando lock...")
                sga_global_lock.release()
        else:
            logger.info("[keep_system_active] Dispositivo ocupado por API.")

    # Resetear estado de actividad
    reset_activity_status()
    
    # Programar la siguiente ejecución
    delay = random.randint(25, 40)
    logger.info(f"[keep_system_active] Programando siguiente ejecución en {delay} segundos...")
    keep_system_active_task.apply_async(countdown=delay)