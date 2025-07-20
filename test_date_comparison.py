#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.modules.sga.minpub.report_validator.service.objetivos.utils.calculations import extract_date_range_last, extract_date_range_last_v2

def test_date_extraction_comparison():
    # Texto de prueba basado en el ejemplo proporcionado
    test_text = """A través de los Sistemas de Monitoreo de Claro, de manera proactiva se identificó la pérdida de gestión del servicio de datos identificado con el CUISMP 18022, y se generó un ticket el día 14/07/2025 a las 05:51 horas. Inmediatamente, Claro procedió con la revisión del enlace, encontrando un inconveniente originado por motivos ajenos a nuestro control, debido a una interrupción en el suministro eléctrico (corte de energía comercial no programado) en la zona donde se ubica nuestro punto de presencia, el cual aprovisiona los servicios del cliente. Claro actuó diligentemente, ya que el respaldo eléctrico funcionó de manera adecuada; sin embargo, se presentó una falla en el sistema de contingencia eléctrica, lo que generó la afectación en los servicios del cliente. Ante ello, se tomaron acciones correctivas inmediatas, desplazando personal técnico especializado para atender la incidencia, logrando restablecer el fluido eléctrico en el punto de presencia. Finalmente, luego de los correctivos, se verificó la operatividad y estabilidad del servicio el día 14/07/2025 a las 08:10 horas.

Fecha y Hora de Inicio: 14/07/2025 a las 05:51 horas.

Fecha y Hora de Fin: 14/07/2025 a las 08:10 horas."""

    print("Texto de prueba:")
    print("=" * 80)
    print(test_text)
    print("=" * 80)
    
    # Probar la función original
    print("\n🔍 FUNCIÓN ORIGINAL:")
    inicio_orig, fin_orig = extract_date_range_last(test_text)
    print(f"Inicio: {inicio_orig}")
    print(f"Fin: {fin_orig}")
    
    # Probar la función nueva
    print("\n🆕 FUNCIÓN NUEVA:")
    inicio_new, fin_new = extract_date_range_last_v2(test_text)
    print(f"Inicio: {inicio_new}")
    print(f"Fin: {fin_new}")
    
    # Verificar que las fechas son correctas
    expected_inicio = "14/07/2025 05:51"
    expected_fin = "14/07/2025 08:10"
    
    print(f"\n✅ RESULTADO ESPERADO:")
    print(f"Inicio: {expected_inicio}")
    print(f"Fin: {expected_fin}")
    
    print(f"\n📊 COMPARACIÓN:")
    print(f"Original funciona: {inicio_orig == expected_inicio and fin_orig == expected_fin}")
    print(f"Nueva funciona: {inicio_new == expected_inicio and fin_new == expected_fin}")
    
    # Análisis del problema
    print(f"\n🔍 ANÁLISIS DEL PROBLEMA:")
    print(f"En tu texto, entre '14/07/2025' y '05:51' hay: ' a las '")
    print(f"Esto son {len(' a las ')} caracteres, que es más que los 10 permitidos por \\D{{0,10}}")
    print(f"Por eso la función original no funciona.")

if __name__ == "__main__":
    test_date_extraction_comparison() 