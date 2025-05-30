import threading
import time
import pyautogui
import random

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import (
    sga, oplogin, newCallCenter, semaforo, reporteCombinado,
    sharepoint_Horario_mesa_atcorp, sharepoint_horario_general_atcorp,
    minpub, pronatel
)

lock = threading.Lock()


def mover_cuadrado(x0, y0, step):
    pyautogui.moveTo(x0, y0)
    time.sleep(0.1)
    pyautogui.moveTo(x0 + step, y0)
    time.sleep(0.1)
    pyautogui.moveTo(x0 + step, y0 + step)
    time.sleep(0.1)
    pyautogui.moveTo(x0, y0 + step)
    time.sleep(0.1)
    pyautogui.moveTo(x0, y0)

def mover_cruz(x0, y0, step):
    pyautogui.moveTo(x0, y0)
    time.sleep(0.1)
    pyautogui.moveTo(x0 + step, y0)
    time.sleep(0.1)
    pyautogui.moveTo(x0, y0)
    time.sleep(0.1)
    pyautogui.moveTo(x0, y0 + step)
    time.sleep(0.1)
    pyautogui.moveTo(x0, y0)

def mover_zigzag(x0, y0, step):
    for i in range(3):
        pyautogui.moveTo(x0 + i * step, y0 + (i % 2) * step)
        time.sleep(0.1)

def mover_diagonal(x0, y0, step):
    for i in range(5):
        pyautogui.moveTo(x0 + i * 10, y0 + i * 10)
        time.sleep(0.1)

def mantener_activo():
    screen_width, screen_height = pyautogui.size()

    x0 = int(screen_width * 0.1)
    y0 = int(screen_height * 0.1)
    step = 50

    last_pos = pyautogui.position()
    patrones = [mover_cuadrado, mover_cruz, mover_zigzag, mover_diagonal]

    while True:
        current_pos = pyautogui.position()

        if current_pos != last_pos:
            print("[mantener_activo] Usuario movió el mouse, no hago nada.")
        else:
            if lock.acquire(blocking=False):
                try:
                    patron = random.choice(patrones)
                    print(f"[mantener_activo] Ejecutando patrón aleatorio: {patron.__name__}")
                    patron(x0, y0, step)
                finally:
                    lock.release()
            else:
                print("[mantener_activo] Mouse ocupado por una tarea crítica. Esperando...")

        last_pos = pyautogui.position()
        delay = random.randint(25, 40)
        print(f"[mantener_activo] Esperando {delay} segundos antes del próximo movimiento.")
        time.sleep(delay)


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