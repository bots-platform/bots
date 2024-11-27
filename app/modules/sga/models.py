from pydantic import BaseModel
from datetime import datetime


class FechaSecuenciaRequest(BaseModel):
    fecha_secuencia_inicio: str
    fecha_secuencia_fin: str