import threading
import time
import pyautogui

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import (
    sga, oplogin, newCallCenter, semaforo, reporteCombinado,
    sharepoint_Horario_mesa_atcorp, sharepoint_horario_general_atcorp,
    minpub, pronatel
)

lock = threading.Lock()

def mantener_activo():
    while True:
        if lock.acquire(blocking=False):
            try:
                print("[mantener_activo] Mouse libre, moviendo...")
                pyautogui.moveRel(1, 0, duration=0.1)
                pyautogui.moveRel(-1, 0, duration=0.1)
            finally:
                lock.release()
        else:
            print("[mantener_activo] Mouse ocupado por API, esperando...")
        time.sleep(30)

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