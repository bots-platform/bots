#!/bin/bash

# Script para verificar puertos disponibles
echo "🔍 Verificando puertos disponibles..."

# Puertos a verificar
PORTS=("3000" "6379" "5432" "8000" "80" "443")

for port in "${PORTS[@]}"; do
    if netstat -tuln | grep -q ":$port "; then
        echo "❌ Puerto $port está en uso"
        echo "   Proceso usando el puerto:"
        netstat -tulnp | grep ":$port "
    else
        echo "✅ Puerto $port está disponible"
    fi
done

echo ""
echo "💡 Si hay puertos en uso, usa:"
echo "   docker-compose -f docker-compose.user.yml up -d --build"
echo "   (usa puertos específicos para evitar conflictos)" 