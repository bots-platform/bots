import threading
from app.shared.lock import global_lock
import time
import random
from pywinauto.keyboard import send_keys
from pywinauto.mouse import click
from pynput import mouse, keyboard

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import (
    sga, oplogin, newCallCenter, semaforo, reporteCombinado,
    sharepoint_Horario_mesa_atcorp, sharepoint_horario_general_atcorp,
    minpub, pronatel
)

lock = global_lock

actividad_humana = {"mouse": False, "teclado": False}

def on_mouse_move(x, y):
    actividad_humana["mouse"] = True

def on_key_press(key):
    actividad_humana["teclado"] = True


def mantener_activo():
    patrones = ["win_key", "click"]

    while True:
        if actividad_humana["mouse"] or actividad_humana["teclado"]:
            print("[mantener_activo] Usuario está activo, no simulo nada.")
        else:
            if lock.acquire(blocking=False):
                try:
                    accion = random.choice(patrones)
                    if accion == "win_key":
                        print("[mantener_activo] Presionando tecla Windows")
                        send_keys("{VK_LWIN}")
                    elif accion == "click":
                        print("[mantener_activo] Clic en (5,5)")
                        click(button='left', coords=(5, 5))
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
app.include_router(sharepoint_Horario_mesa_atcorp.router)
app.include_router(minpub.router)
app.include_router(pronatel.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)