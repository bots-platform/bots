from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # Celery settings
    CELERY_BROKER_URL: str = "redis://localhost:6379/0"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/0"
    
    # Task queue settings
    TASK_QUEUE_NAME: str = "default"
    
    # Automation settings
    SELENIUM_DRIVER_PATH: Optional[str] = None
    PYWINAUTO_TIMEOUT: int = 30
    
    class Config:
        env_file = ".env"

settings = Settings() 