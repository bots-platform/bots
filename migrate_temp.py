#!/usr/bin/env python3
"""
Script temporal para migración de JSON a PostgreSQL
"""

import os
import sys
from pathlib import Path

# Agregar el directorio actual al path
sys.path.append(str(Path(__file__).parent))

# Importar la función de migración
from app.models.simple_migration import run_simple_migration

if __name__ == "__main__":
    print("🚀 Ejecutando migración desde script temporal...")
    run_simple_migration() 