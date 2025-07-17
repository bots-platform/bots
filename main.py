import threading
from app.shared.lock import global_lock
#from app.shared.activity_monitor import activity_monitor
import time
import random
#from pywinauto.keyboard import send_keys
#from pywinauto.mouse import click
#from app.tasks.automation_tasks import keep_system_active_task
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from contextlib import asynccontextmanager
from fastapi import FastAPI

from fastapi.middleware.cors import CORSMiddleware
from app.core.init_db import init_database

from app.api import (
    sga_router, oplogin_router, newCallCenter_router, semaforo_router,
    reporteCombinado_router, sharepoint_horario_general_router,
    sharepoint_horario_mesa_router, minpub_router, pronatel_router,
    auth_router, users_router, permissions_router
)
from app.api.auth_db import router as auth_db_router
from app.api.users_db import router as users_db_router

lock = global_lock

#activity_monitor.start()

#logger.info("Iniciando tarea keep_system_active_task...")
#result = keep_system_active_task.delay()
#logger.info(f"Tarea keep_system_active_task iniciada con ID: {result.id}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicializar base de datos al arrancar
    init_database()
    yield

app = FastAPI(
    title="RPA Bots API",
    description="API for RPA Bots Management System",
    version="1.0.0",
    lifespan=lifespan
)

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

# app.include_router(auth_router)
# app.include_router(users_router)
# app.include_router(permissions_router)

# Nuevas APIs con base de datos (comentar las anteriores cuando esté listo)
app.include_router(auth_db_router)
app.include_router(users_db_router)

@app.get("/")
async def root():
    return {"message": "Welcome to RPA Bots API"}

@app.get("/health")
async def health_check():
    """Health check endpoint for Docker and load balancers"""
    return {
        "status": "healthy",
        "service": "RPA Bots API",
        "version": "1.0.0",
        "timestamp": time.time()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)