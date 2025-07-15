#!/bin/bash

# Full Application Deployment Script
# This script deploys the complete application (backend + frontend)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting Full Application deployment...${NC}"

# Check if .env file exists
if [ ! -f .env ]; then
    echo -e "${YELLOW}⚠️  .env file not found. Creating from example...${NC}"
    cp env.example .env
    echo -e "${RED}❌ Please edit .env file with your production values before continuing${NC}"
    exit 1
fi

# Load environment variables
source .env

# Validate required environment variables
if [ -z "$POSTGRES_PASSWORD" ] || [ "$POSTGRES_PASSWORD" = "your_secure_password_here" ]; then
    echo -e "${RED}❌ POSTGRES_PASSWORD not set in .env file${NC}"
    exit 1
fi

if [ -z "$SECRET_KEY" ] || [ "$SECRET_KEY" = "your-super-secret-key-change-this-in-production" ]; then
    echo -e "${RED}❌ SECRET_KEY not set in .env file${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Environment variables validated${NC}"

# Stop existing containers
echo -e "${YELLOW}🛑 Stopping existing containers...${NC}"
docker-compose -f docker-compose.full.yml down

# Build and start all services
echo -e "${YELLOW}🔨 Building and starting all services...${NC}"
docker-compose -f docker-compose.full.yml up -d --build

# Wait for services to be healthy
echo -e "${YELLOW}⏳ Waiting for services to be healthy...${NC}"
sleep 45

# Check service health
echo -e "${YELLOW}🏥 Checking service health...${NC}"
docker-compose -f docker-compose.full.yml ps

# Test all endpoints
echo -e "${YELLOW}🔍 Testing all endpoints...${NC}"

# Test backend health
if curl -f http://localhost:8000/health > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Backend is healthy${NC}"
else
    echo -e "${RED}❌ Backend health check failed${NC}"
fi

# Test frontend health
if curl -f http://localhost:80/health > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Frontend is healthy${NC}"
else
    echo -e "${RED}❌ Frontend health check failed${NC}"
fi

echo -e "${GREEN}🎉 Full application deployment completed successfully!${NC}"
echo -e "${GREEN}📱 Frontend: http://localhost${NC}"
echo -e "${GREEN}🔧 Backend API: http://localhost:8000${NC}"
echo -e "${GREEN}📊 Health Check: http://localhost/health${NC}"
echo -e "${GREEN}🗄️  Database: localhost:5432${NC}"
echo -e "${GREEN}🔴 Redis: localhost:6379${NC}"

# Show logs
echo -e "${YELLOW}📋 Recent logs:${NC}"
docker-compose -f docker-compose.full.yml logs --tail=20

echo -e "${YELLOW}💡 Useful commands:${NC}"
echo -e "${YELLOW}   - View logs: docker-compose -f docker-compose.full.yml logs -f${NC}"
echo -e "${YELLOW}   - Stop all: docker-compose -f docker-compose.full.yml down${NC}"
echo -e "${YELLOW}   - Restart backend: docker-compose -f docker-compose.full.yml restart backend${NC}"
echo -e "${YELLOW}   - Restart frontend: docker-compose -f docker-compose.full.yml restart frontend${NC}" 