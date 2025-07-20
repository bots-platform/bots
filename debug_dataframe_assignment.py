#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from app.modules.sga.minpub.report_validator.service.objetivos.utils.calculations import extract_date_range_last

def debug_dataframe_assignment():
    # Crear un DataFrame de prueba
    test_data = {
        'it_medidas_tomadas': [
            # Caso 1: Texto que funciona
            """A través de los Sistemas de Monitoreo de Claro, de manera proactiva se identificó la pérdida de gestión del servicio de datos identificado con el CUISMP 18022, y se generó un ticket el día 14/07/2025 a las 05:51 horas. Inmediatamente, Claro procedió con la revisión del enlace, encontrando un inconveniente originado por motivos ajenos a nuestro control, debido a una interrupción en el suministro eléctrico (corte de energía comercial no programado) en la zona donde se ubica nuestro punto de presencia, el cual aprovisiona los servicios del cliente. Claro actuó diligentemente, ya que el respaldo eléctrico funcionó de manera adecuada; sin embargo, se presentó una falla en el sistema de contingencia eléctrica, lo que generó la afectación en los servicios del cliente. Ante ello, se tomaron acciones correctivas inmediatas, desplazando personal técnico especializado para atender la incidencia, logrando restablecer el fluido eléctrico en el punto de presencia. Finalmente, luego de los correctivos, se verificó la operatividad y estabilidad del servicio el día 14/07/2025 a las 08:10 horas.

Fecha y Hora de Inicio: 14/07/2025 a las 05:51 horas.

Fecha y Hora de Fin: 14/07/2025 a las 08:10 horas.""",
            
            # Caso 2: Texto que no tiene fechas
            "Este es un texto sin fechas",
            
            # Caso 3: Texto vacío
            "",
            
            # Caso 4: None
            None
        ]
    }
    
    df = pd.DataFrame(test_data)
    
    print("DataFrame original:")
    print(df)
    print("\n" + "="*80)
    
    # Aplicar la función
    print("Aplicando extract_date_range_last...")
    date_range_it_last = df['it_medidas_tomadas'].apply(extract_date_range_last)
    
    print("\nResultado de apply:")
    for i, result in enumerate(date_range_it_last):
        print(f"Fila {i}: {result} (tipo: {type(result)})")
    
    print("\n" + "="*80)
    
    # Intentar crear el DataFrame
    print("Intentando crear DataFrame con .tolist():")
    try:
        result_df = pd.DataFrame(date_range_it_last.tolist(), index=df.index)
        print("✅ Éxito al crear DataFrame:")
        print(result_df)
    except Exception as e:
        print(f"❌ Error al crear DataFrame: {e}")
    
    print("\n" + "="*80)
    
    # Verificar tipos de datos
    print("Verificando tipos de datos:")
    for i, result in enumerate(date_range_it_last):
        if result is not None:
            print(f"Fila {i}: {result} - Inicio: {type(result[0])}, Fin: {type(result[1])}")
        else:
            print(f"Fila {i}: None")

if __name__ == "__main__":
    debug_dataframe_assignment() 