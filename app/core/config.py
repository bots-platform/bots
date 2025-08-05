# app/core/config.py
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    CELERY_BROKER_URL: Optional[str] = None
    CELERY_RESULT_BACKEND: Optional[str] = None

    TASK_QUEUE_NAME: str = "default"

    URL_OPLOGIN:      str
    OPLOGIN_USER:     str
    OPLOGIN_PASSWORD: str
    URL_NEW_CALL_CENTER: str
    NEW_CALL_CENTER_USER: str
    NEW_CALL_CENTER_PASSWORD: str
    URL_SEMAFORO: str
    SEMAFORO_USER: str
    SEMAFORO_PASSWORD: str
    URL_SHAREPOINT: str
    SHAREPOINT_USER: str
    SHAREPOINT_PASSWORD: str
    URL_DJANGO: str
    AUTH_USERNAME: str
    AUTH_PASSWORD: str
    EXCEL_FILENAME: str
    EXCEL_CONTENT_TYPE: str
    EXCEL_PATH: str


    PYWINAUTO_TIMEOUT: int = 30
    SELENIUM_DRIVER_PATH: Optional[str] = None

    class Config:
        env_file = ".env"
        extra = "ignore"      

settings = Settings()
