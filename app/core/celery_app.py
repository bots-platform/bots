from celery import Celery
from app.core.config import settings

celery_app = Celery(
    "worker",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=["app.tasks"]
)

# Optional configuration
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


    task_routes={
        "app.tasks.automation_tasks.process_minpub_task": {"queue": "ui"},
        "app.tasks.automation_tasks.process_sga_report_task": {"queue": "ui"},
        "app.tasks.automation_tasks.keep_system_active_task": {"queue": "ui"},
        # "app.tasks.automation_tasks.process_minpub_task": {"queue": "ui"},
        # "app.tasks.automation_tasks.process_minpub_task": {"queue": "ui"},
    }


) 