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

from contextlib import asynccontextmanager
from fastapi import FastAPI


from fastapi.middleware.cors import CORSMiddleware


from app.api import (
    sga_router, oplogin_router, newCallCenter_router, semaforo_router,
    reporteCombinado_router, sharepoint_horario_general_router,
    sharepoint_horario_mesa_router, minpub_router, pronatel_router,
    auth_router, users_router, permissions_router
)

lock = global_lock

# actividad_humana = {"mouse": False, "teclado": False}

# def on_mouse_move(x, y):
#     actividad_humana["mouse"] = True
#     logger.info("Actividad de mouse detectada")

# def on_key_press(key):
#     actividad_humana["teclado"] = True
#     logger.info("Actividad de teclado detectada")

# # Iniciar los listeners de mouse y teclado
# mouse_listener = mouse.Listener(on_move=on_mouse_move)
# keyboard_listener = keyboard.Listener(on_press=on_key_press)

# mouse_listener.start()
# keyboard_listener.start()

# # Iniciar la tarea de Celery para mantener el sistema activo
# logger.info("Iniciando tarea keep_system_active_task...")
# result = keep_system_active_task.delay()
# logger.info(f"Tarea keep_system_active_task iniciada con ID: {result.id}")





app = FastAPI(
    title="RPA Bots API",
    description="API for RPA Bots Management System",
    version="1.0.0",

)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(sga_router)
app.include_router(oplogin_router)
app.include_router(newCallCenter_router)
app.include_router(semaforo_router)
app.include_router(reporteCombinado_router)
app.include_router(sharepoint_horario_general_router)
app.include_router(sharepoint_horario_mesa_router)
app.include_router(minpub_router)
app.include_router(pronatel_router)

app.include_router(auth_router)
app.include_router(users_router)
app.include_router(permissions_router)

@app.get("/")
async def root():
    return {"message": "Welcome to RPA Bots API"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)