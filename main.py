import threading
from app.shared.lock import global_lock
import time
import random
from pywinauto.keyboard import send_keys
from pywinauto.mouse import click
from pynput import mouse, keyboard

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.database import engine
from app import models
from app.api import (
    sga_router, oplogin_router, newCallCenter_router, semaforo_router,
    reporteCombinado_router, sharepoint_horario_general_router,
    sharepoint_horario_mesa_router, minpub_router, pronatel_router,
    auth_router, users_router, permissions_router
)

lock = global_lock

actividad_humana = {"mouse": False, "teclado": False}

def on_mouse_move(x, y):
    actividad_humana["mouse"] = True

def on_key_press(key):
    actividad_humana["teclado"] = True


def mantener_activo():
    patrones = ["click_1", "click_2"]

    while True:
        if actividad_humana["mouse"] or actividad_humana["teclado"]:
            print("[mantener_activo] Usuario está activo, no simulo nada.")
        else:
            if lock.acquire(blocking=False):
                try:
                    accion = random.choice(patrones)
                    if accion == "click_1":
                        print("[mantener_activo] Clic en (35,10)")
                        click(button='left', coords=(35, 10))
                    elif accion == "click_2":
                        print("[mantener_activo] Clic en (15,5)")
                        click(button='left', coords=(15, 5))
                finally:
                    lock.release()
            else:
                print("[mantener_activo] Dispositivo ocupado por API.")

 
        actividad_humana["mouse"] = False
        actividad_humana["teclado"] = False

        delay = random.randint(25, 40)
        print(f"[mantener_activo] Esperando {delay} segundos...")
        time.sleep(delay)


mouse_listener = mouse.Listener(on_move=on_mouse_move)
keyboard_listener = keyboard.Listener(on_press=on_key_press)

mouse_listener.start()
keyboard_listener.start()

threading.Thread(target=mantener_activo, daemon=True).start()

# Create database tables
models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="RPA Bots API",
    description="API for RPA Bots Management System",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include existing routers
app.include_router(sga_router)
app.include_router(oplogin_router)
app.include_router(newCallCenter_router)
app.include_router(semaforo_router)
app.include_router(reporteCombinado_router)
app.include_router(sharepoint_horario_general_router)
app.include_router(sharepoint_horario_mesa_router)
app.include_router(minpub_router)
app.include_router(pronatel_router)

# Include new authentication and user management routers
app.include_router(auth_router)
app.include_router(users_router)
app.include_router(permissions_router)

@app.get("/")
async def root():
    return {"message": "Welcome to RPA Bots API"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)