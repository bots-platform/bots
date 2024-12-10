from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api import sga, oplogin, newCallCenter, semaforo, reporteCombinado


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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)