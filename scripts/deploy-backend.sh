#!/bin/bash

# Backend Deployment Script
# This script deploys only the backend services

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting Backend deployment...${NC}"

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

# Stop existing backend containers
echo -e "${YELLOW}🛑 Stopping existing backend containers...${NC}"
docker-compose down

# Build and start backend services
echo -e "${YELLOW}🔨 Building and starting backend services...${NC}"
docker-compose up -d --build

# Wait for services to be healthy
echo -e "${YELLOW}⏳ Waiting for services to be healthy...${NC}"
sleep 30

# Check service health
echo -e "${YELLOW}🏥 Checking service health...${NC}"
docker-compose ps

# Test backend health
echo -e "${YELLOW}🔍 Testing backend endpoints...${NC}"
if curl -f http://localhost:8000/health > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Backend is healthy${NC}"
else
    echo -e "${RED}❌ Backend health check failed${NC}"
fi

echo -e "${GREEN}🎉 Backend deployment completed successfully!${NC}"
echo -e "${GREEN}🔧 Backend API: http://localhost:8000${NC}"
echo -e "${GREEN}🗄️  Database: localhost:5432${NC}"
echo -e "${GREEN}🔴 Redis: localhost:6379${NC}"

# Show logs
echo -e "${YELLOW}📋 Recent backend logs:${NC}"
docker-compose logs --tail=10 