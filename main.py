import threading
from app.shared.lock import global_lock
import time
import random
from pywinauto.keyboard import send_keys
from pywinauto.mouse import click
from pynput import mouse, keyboard
from app.tasks.automation_tasks import keep_system_active_task
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import (
    sga, oplogin, newCallCenter, semaforo, reporteCombinado,
    sharepoint_horario_mesa_atcorp, sharepoint_horario_general_atcorp,
    minpub, pronatel
)

lock = global_lock

actividad_humana = {"mouse": False, "teclado": False}

def on_mouse_move(x, y):
    actividad_humana["mouse"] = True
    logger.info("Actividad de mouse detectada")

def on_key_press(key):
    actividad_humana["teclado"] = True
    logger.info("Actividad de teclado detectada")

# Iniciar los listeners de mouse y teclado
mouse_listener = mouse.Listener(on_move=on_mouse_move)
keyboard_listener = keyboard.Listener(on_press=on_key_press)

mouse_listener.start()
keyboard_listener.start()

# Iniciar la tarea de Celery para mantener el sistema activo
logger.info("Iniciando tarea keep_system_active_task...")
result = keep_system_active_task.delay()
logger.info(f"Tarea keep_system_active_task iniciada con ID: {result.id}")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(sga.router)
app.include_router(oplogin.router)
app.include_router(newCallCenter.router)
app.include_router(semaforo.router)
app.include_router(reporteCombinado.router)
app.include_router(sharepoint_horario_general_atcorp.router)
app.include_router(sharepoint_horario_mesa_atcorp.router)
app.include_router(minpub.router)
app.include_router(pronatel.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)