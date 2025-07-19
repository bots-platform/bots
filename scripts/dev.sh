#!/bin/bash

# RPA Bots Development Script
# This script starts the development environment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting RPA Bots development environment...${NC}"

# Check if .env file exists
if [ ! -f .env ]; then
    echo -e "${YELLOW}⚠️  .env file not found. Creating from example...${NC}"
    cp env.example .env
    echo -e "${YELLOW}📝 Please edit .env file with your development values${NC}"
fi

# Stop existing containers
echo -e "${YELLOW}🛑 Stopping existing containers...${NC}"
docker-compose -f docker-compose.dev.yml down

# Build and start development services
echo -e "${YELLOW}🔨 Building and starting development services...${NC}"
docker-compose -f docker-compose.dev.yml up -d --build

# Wait for services to start
echo -e "${YELLOW}⏳ Waiting for services to start...${NC}"
sleep 15

# Check service health
echo -e "${YELLOW}🏥 Checking service health...${NC}"
docker-compose -f docker-compose.dev.yml ps

echo -e "${GREEN}🎉 Development environment started!${NC}"
echo -e "${GREEN}📱 Frontend: http://localhost:3000${NC}"
echo -e "${GREEN}🔧 Backend API: http://localhost:8000${NC}"
echo -e "${GREEN}🗄️  Database: localhost:5432${NC}"
echo -e "${GREEN}🔴 Redis: localhost:6379${NC}"

# Show logs
echo -e "${YELLOW}📋 Recent logs:${NC}"
docker-compose -f docker-compose.dev.yml logs --tail=10

echo -e "${YELLOW}💡 To view logs: docker-compose -f docker-compose.dev.yml logs -f${NC}"
echo -e "${YELLOW}💡 To stop: docker-compose -f docker-compose.dev.yml down${NC}" 