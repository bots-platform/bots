from pydantic import BaseModel, ValidationInfo, field_validator, Field
from datetime import date


class FechaReporteActividadAgenteRequest(BaseModel):
    fecha_inicio: date = Field(..., description="La fecha de inicio del rango (YYYY-MM-DD).")
    fecha_fin: date = Field(..., description="La fecha de fin del rango (YYYY-MM-DD).")

    class Config:
        json_schem_extra = {
            "example": {
                "fecha_inicio": "2024-12-01",
                "fecha_fin": "2024-12-31"
            }
        }


    @field_validator("fecha_fin")
    def validar_rango_fecha(cls,fecha_fin, info : ValidationInfo):
        fecha_inicio = info.data.get("fecha_inicio")
        if fecha_inicio and fecha_inicio > fecha_fin:
            raise ValueError("La fecha de inicio no puede ser superior que la fecha de fin")
        return fecha_fin    
