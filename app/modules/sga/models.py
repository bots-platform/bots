from pydantic import BaseModel
from datetime import datetime


class FechaSecuenciaRequest(BaseModel):
    fecha_inicio: str
    fecha_fin: str


    