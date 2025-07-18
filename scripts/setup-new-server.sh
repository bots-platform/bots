#!/bin/bash

# Script para configurar un nuevo servidor RPA Bots
# Este script genera una nueva SECRET_KEY y configura las variables de entorno
# Soporta tanto desarrollo como producción

set -e  # Salir si hay algún error

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Función para imprimir con colores
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${PURPLE}=== $1 ===${NC}"
}

# Función para mostrar el menú de selección
show_menu() {
    echo ""
    print_header "Configuración de Nuevo Servidor RPA Bots"
    echo ""
    echo "Selecciona el entorno de despliegue:"
    echo "1) Desarrollo (docker-compose.dev.yml)"
    echo "2) Producción (docker-compose.full.yml)"
    echo "3) Salir"
    echo ""
    read -p "Ingresa tu opción (1-3): " choice
}

# Función para configurar desarrollo
setup_development() {
    print_header "Configurando entorno de DESARROLLO"
    
    COMPOSE_FILE="docker-compose.dev.yml"
    ENVIRONMENT="development"
    
    print_status "Usando $COMPOSE_FILE"
    
    # Verificar que el archivo existe
    if [ ! -f "$COMPOSE_FILE" ]; then
        print_error "No se encontró $COMPOSE_FILE"
        exit 1
    fi
    
    # Generar configuración
    generate_configuration
}

# Función para configurar producción
setup_production() {
    print_header "Configurando entorno de PRODUCCIÓN"
    
    COMPOSE_FILE="docker-compose.full.yml"
    ENVIRONMENT="production"
    
    print_status "Usando $COMPOSE_FILE"
    
    # Verificar que el archivo existe
    if [ ! -f "$COMPOSE_FILE" ]; then
        print_error "No se encontró $COMPOSE_FILE"
        exit 1
    fi
    
    # Verificar que React-Bots existe para producción
    if [ ! -d "React-Bots" ]; then
        print_warning "No se encontró el directorio React-Bots"
        print_warning "Para producción completa, asegúrate de tener el frontend"
    fi
    
    # Generar configuración
    generate_configuration
}

# Función para generar la configuración
generate_configuration() {
    print_status "Verificando dependencias..."
    
    # Verificar si estamos en el directorio correcto
    if [ ! -f "docker-compose.dev.yml" ]; then
        print_error "No se encontró docker-compose.dev.yml. Asegúrate de estar en el directorio raíz del proyecto."
        exit 1
    fi
    
    # Verificar si Docker está instalado
    if ! command -v docker &> /dev/null; then
        print_error "Docker no está instalado. Por favor instala Docker primero."
        exit 1
    fi
    
    # Verificar si Docker Compose está disponible
    if ! docker-compose --version &> /dev/null; then
        print_error "Docker Compose no está disponible. Por favor instala Docker Compose."
        exit 1
    fi
    
    # Generar nueva SECRET_KEY
    print_status "Generando nueva SECRET_KEY..."
    NEW_SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(32))")
    
    if [ -z "$NEW_SECRET_KEY" ]; then
        print_error "No se pudo generar la SECRET_KEY. Verifica que Python3 esté instalado."
        exit 1
    fi
    
    print_success "SECRET_KEY generada exitosamente"
    
    # Generar contraseña para PostgreSQL
    print_status "Generando contraseña para PostgreSQL..."
    POSTGRES_PASSWORD=$(openssl rand -base64 12 | tr -d "=+/" | cut -c1-16)
    
    if [ -z "$POSTGRES_PASSWORD" ]; then
        print_warning "No se pudo generar contraseña con OpenSSL, usando contraseña por defecto"
        POSTGRES_PASSWORD="admin"
    fi
    
    print_success "Contraseña PostgreSQL generada"
    
    # Generar contraseña de admin para producción
    if [ "$ENVIRONMENT" = "production" ]; then
        print_status "Generando contraseña de administrador..."
        ADMIN_PASSWORD=$(openssl rand -base64 8 | tr -d "=+/" | cut -c1-12)
        
        if [ -z "$ADMIN_PASSWORD" ]; then
            print_warning "No se pudo generar contraseña de admin, usando contraseña por defecto"
            ADMIN_PASSWORD="admin123"
        fi
        
        print_success "Contraseña de administrador generada"
    fi
    
    # Crear archivo .env
    print_status "Creando archivo .env..."
    
    if [ "$ENVIRONMENT" = "production" ]; then
        cat > .env << EOF
# Configuración del servidor RPA Bots - PRODUCCIÓN
# Este archivo contiene variables sensibles - NO subir a Git

# Clave secreta para JWT tokens
SECRET_KEY=$NEW_SECRET_KEY

# Configuración de PostgreSQL
POSTGRES_PASSWORD=$POSTGRES_PASSWORD

# Configuración de administrador (solo para producción)
ADMIN_PASSWORD=$ADMIN_PASSWORD

# Configuración adicional
ENVIRONMENT=production
ACCESS_TOKEN_EXPIRE_MINUTES=30
EOF
    else
        cat > .env << EOF
# Configuración del servidor RPA Bots - DESARROLLO
# Este archivo contiene variables sensibles - NO subir a Git

# Clave secreta para JWT tokens
SECRET_KEY=$NEW_SECRET_KEY

# Configuración de PostgreSQL
POSTGRES_PASSWORD=$POSTGRES_PASSWORD

# Configuración adicional (opcional)
# ACCESS_TOKEN_EXPIRE_MINUTES=30
# ENVIRONMENT=development
EOF
    fi
    
    print_success "Archivo .env creado exitosamente"
    
    # Verificar que el archivo .env se creó correctamente
    if [ ! -f ".env" ]; then
        print_error "No se pudo crear el archivo .env"
        exit 1
    fi
    
    # Mostrar información de configuración
    print_status "Configuración completada:"
    echo ""
    echo "📋 Información de configuración:"
    echo "   • Entorno: $ENVIRONMENT"
    echo "   • Archivo compose: $COMPOSE_FILE"
    echo "   • SECRET_KEY: ${NEW_SECRET_KEY:0:20}..."
    echo "   • POSTGRES_PASSWORD: $POSTGRES_PASSWORD"
    if [ "$ENVIRONMENT" = "production" ]; then
        echo "   • ADMIN_PASSWORD: $ADMIN_PASSWORD"
    fi
    echo "   • Archivo .env: ✅ Creado"
    echo ""
    
    # Verificar si existe el script de migración
    if [ -f "scripts/migrate-database.sh" ]; then
        print_status "Script de migración encontrado"
        echo ""
        echo "📋 Próximos pasos:"
        echo "   1. Ejecutar: ./scripts/migrate-database.sh"
        echo "   2. Ejecutar: docker-compose -f $COMPOSE_FILE up -d"
        echo "   3. Verificar logs: docker-compose -f $COMPOSE_FILE logs -f"
        echo ""
    else
        print_warning "No se encontró scripts/migrate-database.sh"
        echo ""
        echo "📋 Próximos pasos:"
        echo "   1. Crear las tablas de la base de datos manualmente"
        echo "   2. Ejecutar: docker-compose -f $COMPOSE_FILE up -d"
        echo "   3. Verificar logs: docker-compose -f $COMPOSE_FILE logs -f"
        echo ""
    fi
    
    # Verificar permisos del script de migración
    if [ -f "scripts/migrate-database.sh" ]; then
        chmod +x scripts/migrate-database.sh
        print_success "Permisos de ejecución otorgados al script de migración"
    fi
    
    print_success "¡Configuración del nuevo servidor completada!"
    echo ""
    echo "🔐 IMPORTANTE:"
    echo "   • Guarda la SECRET_KEY y POSTGRES_PASSWORD en un lugar seguro"
    if [ "$ENVIRONMENT" = "production" ]; then
        echo "   • Guarda la ADMIN_PASSWORD en un lugar seguro"
    fi
    echo "   • No compartas el archivo .env"
    echo "   • Cada servidor debe tener su propia SECRET_KEY única"
    echo ""
    
    # Mostrar comandos específicos para el entorno
    if [ "$ENVIRONMENT" = "production" ]; then
        echo "🚀 Comandos para producción:"
        echo "   • Iniciar servicios: docker-compose -f $COMPOSE_FILE up -d"
        echo "   • Ver logs: docker-compose -f $COMPOSE_FILE logs -f"
        echo "   • Detener servicios: docker-compose -f $COMPOSE_FILE down"
        echo "   • Reiniciar: docker-compose -f $COMPOSE_FILE restart"
    else
        echo "🚀 Comandos para desarrollo:"
        echo "   • Iniciar servicios: docker-compose -f $COMPOSE_FILE up -d"
        echo "   • Ver logs: docker-compose -f $COMPOSE_FILE logs -f"
        echo "   • Detener servicios: docker-compose -f $COMPOSE_FILE down"
        echo "   • Reiniciar: docker-compose -f $COMPOSE_FILE restart"
    fi
    echo ""
}

# Función principal
main() {
    echo "🚀 Configurando nuevo servidor RPA Bots..."
    
    # Mostrar menú
    show_menu
    
    case $choice in
        1)
            setup_development
            ;;
        2)
            setup_production
            ;;
        3)
            print_status "Saliendo..."
            exit 0
            ;;
        *)
            print_error "Opción inválida. Por favor selecciona 1, 2 o 3."
            exit 1
            ;;
    esac
}

# Ejecutar función principal
main 