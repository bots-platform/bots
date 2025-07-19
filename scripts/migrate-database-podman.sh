#!/bin/bash

# Script de migración de base de datos para Podman
# Ejecuta la migración de usuarios y permisos de JSON a PostgreSQL

set -e

echo "🚀 Iniciando migración de base de datos con Podman..."

# Verificar que estamos en el directorio correcto
if [ ! -f "main.py" ]; then
    echo "❌ Error: Debes ejecutar este script desde el directorio raíz del proyecto"
    exit 1
fi

# Verificar que Podman esté ejecutándose
if ! podman info > /dev/null 2>&1; then
    echo "❌ Error: Podman no está ejecutándose"
    exit 1
fi

# Verificar que los contenedores estén ejecutándose
if ! podman ps | grep -q "rpa_postgres"; then
    echo "❌ Error: El contenedor de PostgreSQL no está ejecutándose"
    echo "💡 Ejecuta: podman-compose -f docker-compose.user.yml up -d postgres"
    exit 1
fi

echo "📡 Verificando conexión a PostgreSQL..."
sleep 5  # Dar tiempo a que PostgreSQL esté listo

# Ejecutar migración dentro del contenedor como root
echo "🔧 Ejecutando migración..."
podman-compose -f docker-compose.user.yml exec --user root backend python -c "
import sys
sys.path.append('/app')
from app.models.simple_migration import run_simple_migration
run_simple_migration()
"

echo "✅ Migración completada exitosamente!"
echo ""
echo "📋 Próximos pasos:"
echo "   1. Prueba el login con los usuarios migrados"
echo "   2. Verifica que todos los usuarios estén en PostgreSQL"
echo "   3. Configura usuarios adicionales si es necesario"
echo "   4. Comenta las APIs JSON antiguas en main.py" 