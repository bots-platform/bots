from celery import Celery
from app.core.config import settings

# Solo inicializar Celery si las variables están configuradas
if settings.CELERY_BROKER_URL and settings.CELERY_RESULT_BACKEND:
    celery_app = Celery(
        "worker",
        broker=settings.CELERY_BROKER_URL,
        backend=settings.CELERY_RESULT_BACKEND,
        include=["app.tasks"]
    )
else:
    # Crear un objeto dummy para evitar errores de importación
    celery_app = None

# Optional configuration
if celery_app:
    celery_app.conf.update(
        task_serializer='json',
        accept_content=['json'],
        result_serializer='json',
        timezone='UTC',
        enable_utc=True,
        task_track_started=True,
        task_time_limit=3600,  # 1 hour timeout
        worker_max_tasks_per_child=200,  # Restart worker after 200 tasks
        worker_prefetch_multiplier=1,  # Process one task at a time
        task_reject_on_worker_lost=True,
        
        # Configuración para beat scheduler
        # Usar scheduler en memoria para evitar problemas de permisos
        beat_scheduler='celery.beat.Scheduler',
        beat_sync_every=1,  # Sincronizar cada tarea
        beat_max_loop_interval=5,  # Máximo intervalo de loop

        task_routes={
            "app.tasks.automation_tasks.process_minpub_task": {"queue": "ui"},
            "app.tasks.automation_tasks.process_sga_report_task": {"queue": "ui"},
            "app.tasks.automation_tasks.keep_system_active_task": {"queue": "ui"},
            # "app.tasks.automation_tasks.process_minpub_task": {"queue": "ui"},
            # "app.tasks.automation_tasks.process_minpub_task": {"queue": "ui"},
        }
    ) 