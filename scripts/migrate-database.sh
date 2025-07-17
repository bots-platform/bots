#!/bin/bash

# Script de migración de base de datos
# Ejecuta la migración de usuarios y permisos de JSON a PostgreSQL

set -e

echo "🚀 Iniciando migración de base de datos..."

# Verificar que estamos en el directorio correcto
if [ ! -f "main.py" ]; then
    echo "❌ Error: Debes ejecutar este script desde el directorio raíz del proyecto"
    exit 1
fi

# Verificar que Docker esté ejecutándose
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker no está ejecutándose"
    exit 1
fi

# Verificar que los contenedores estén ejecutándose
if ! docker ps | grep -q "rpa_postgres"; then
    echo "❌ Error: El contenedor de PostgreSQL no está ejecutándose"
    echo "💡 Ejecuta: docker-compose up -d postgres"
    exit 1
fi

echo "📡 Verificando conexión a PostgreSQL..."
sleep 5  # Dar tiempo a que PostgreSQL esté listo

# Ejecutar migración dentro del contenedor
echo "🔧 Ejecutando migración..."
#docker-compose -f docker-compose.full.yml exec -w /app backend python app/models/simple_migration.py
docker-compose -f docker-compose.full.yml exec -e PYTHONPATH=/app backend python app/models/simple_migration.py

echo "✅ Migración completada exitosamente!"
echo ""
echo "📋 Próximos pasos:"
echo "   1. Prueba el login con admin/admin123"
echo "   2. Cambia la contraseña del admin"
echo "   3. Configura usuarios adicionales"
echo "   4. Comenta las APIs JSON antiguas en main.py" 