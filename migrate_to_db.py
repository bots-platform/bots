#!/usr/bin/env python3
"""
Script de migración de usuarios y permisos de JSON a PostgreSQL
"""

import os
import sys
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.append(str(Path(__file__).parent))

from app.models.migrations import run_migration
from app.models.database import engine
from app.models import Base
from sqlalchemy import text

def main():
    """Función principal de migración"""
    print("🚀 Iniciando migración de usuarios y permisos a PostgreSQL...")
    print("=" * 60)
    
    try:
        # Verificar conexión a la base de datos
        print("📡 Verificando conexión a la base de datos...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ Conexión a PostgreSQL exitosa")
        
        # Ejecutar migración
        run_migration()
        
        print("\n" + "=" * 60)
        print("🎉 ¡Migración completada exitosamente!")
        print("\n📋 Resumen:")
        print("   ✅ Tablas creadas en PostgreSQL")
        print("   ✅ Usuario admin creado (admin/losmelones)")
        print("   ✅ Permisos iniciales configurados")
        print("   ✅ Datos migrados desde JSON (si existían)")
        
        print("\n🔐 Credenciales de acceso:")
        print("   Usuario: admin")
        print("   Contraseña: losmelones")
        print("   ⚠️  IMPORTANTE: Cambia la contraseña después del primer login!")
        
        print("\n📝 Próximos pasos:")
        print("   1. Actualiza las APIs para usar la nueva base de datos")
        print("   2. Prueba el login con las nuevas credenciales")
        print("   3. Configura usuarios adicionales desde el panel de admin")
        
    except Exception as e:
        print(f"\n❌ Error durante la migración: {e}")
        print("\n🔧 Solución de problemas:")
        print("   1. Verifica que PostgreSQL esté ejecutándose")
        print("   2. Verifica las variables de entorno DATABASE_URL")
        print("   3. Asegúrate de que las credenciales sean correctas")
        sys.exit(1)

if __name__ == "__main__":
    main() 