#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.modules.sga.minpub.report_validator.service.objetivos.utils.calculations import extract_date_range_last

def test_date_extraction():
    # Texto de prueba basado en el ejemplo proporcionado
    test_text = """A través de los Sistemas de Monitoreo de Claro, de manera proactiva se identificó la pérdida de gestión del servicio de datos identificado con el CUISMP 18022, y se generó un ticket el día 14/07/2025 a las 05:51 horas. Inmediatamente, Claro procedió con la revisión del enlace, encontrando un inconveniente originado por motivos ajenos a nuestro control, debido a una interrupción en el suministro eléctrico (corte de energía comercial no programado) en la zona donde se ubica nuestro punto de presencia, el cual aprovisiona los servicios del cliente. Claro actuó diligentemente, ya que el respaldo eléctrico funcionó de manera adecuada; sin embargo, se presentó una falla en el sistema de contingencia eléctrica, lo que generó la afectación en los servicios del cliente. Ante ello, se tomaron acciones correctivas inmediatas, desplazando personal técnico especializado para atender la incidencia, logrando restablecer el fluido eléctrico en el punto de presencia. Finalmente, luego de los correctivos, se verificó la operatividad y estabilidad del servicio el día 14/07/2025 a las 08:10 horas.

Fecha y Hora de Inicio: 14/07/2025 a las 05:51 horas.

Fecha y Hora de Fin: 14/07/2025 a las 08:10 horas."""

    print("Texto de prueba:")
    print("=" * 50)
    print(test_text)
    print("=" * 50)
    
    # Probar la función
    inicio, fin = extract_date_range_last(test_text)
    
    print(f"\nResultado:")
    print(f"Inicio: {inicio}")
    print(f"Fin: {fin}")
    
    # Verificar que las fechas son correctas
    expected_inicio = "14/07/2025 05:51"
    expected_fin = "14/07/2025 08:10"
    
    print(f"\nEsperado:")
    print(f"Inicio: {expected_inicio}")
    print(f"Fin: {expected_fin}")
    
    print(f"\n¿Coincide?: {inicio == expected_inicio and fin == expected_fin}")

if __name__ == "__main__":
    test_date_extraction() 