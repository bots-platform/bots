# 🐳 RPA Bots - Docker Deployment Guide (Modular Architecture)

## 📋 Overview

This project uses a **modular Docker architecture** for maximum scalability and team independence. The structure includes:

- **Backend**: FastAPI with Python 3.10 (bots_rpa/)
- **Frontend**: React with Nginx (React-Bots/)
- **Database**: PostgreSQL 15
- **Cache/Queue**: Redis 7
- **Reverse Proxy**: Nginx with SSL support
- **Task Queue**: Celery with Redis broker

## 🏗️ Modular Architecture

```
📁 Project Root/
├── 📁 bots_rpa/                    # Backend Services
│   ├── docker-compose.yml          # Backend only
│   ├── scripts/deploy-backend.sh   # Backend deployment
│   └── docker/backend/
├── 📁 React-Bots/                  # Frontend Services
│   ├── docker-compose.yml          # Frontend only
│   ├── scripts/deploy-frontend.sh  # Frontend deployment
│   └── docker/frontend/
└── 📄 docker-compose.full.yml      # Complete application
```

## 🚀 Deployment Options

### Option 1: Deploy Everything Together (Recommended for small teams)
```bash
# Deploy complete application
chmod +x scripts/deploy-full.sh
./scripts/deploy-full.sh
```

### Option 2: Deploy Services Independently (Recommended for large teams)
```bash
# Deploy only backend
chmod +x scripts/deploy-backend.sh
./scripts/deploy-backend.sh

# Deploy only frontend (from React-Bots directory)
cd React-Bots
chmod +x scripts/deploy-frontend.sh
./scripts/deploy-frontend.sh
```

### Option 3: Development Environment
```bash
# Backend development
docker-compose -f docker-compose.dev.yml up -d

# Frontend development
cd React-Bots
docker-compose -f docker-compose.dev.yml up -d
```

## 🏗️ Architecture Benefits

### **Why Modular is Better for Scalability:**

1. **Team Independence**
   - Backend team can deploy without frontend
   - Frontend team can deploy without backend
   - Different release cycles

2. **Technology Flexibility**
   - Backend can use different tech stack
   - Frontend can be React, Vue, Angular, etc.
   - Independent versioning

3. **Scalability**
   - Scale backend independently
   - Scale frontend independently
   - Different resource requirements

4. **CI/CD Independence**
   - Separate pipelines for each service
   - Independent testing
   - Faster deployments

## 🚀 Quick Start

### Prerequisites
- Docker Engine 20.10+
- Docker Compose 2.0+
- At least 4GB RAM available

### 1. Setup Environment
```bash
# Clone repository
git clone <your-repo-url>
cd bots_rpa

# Copy environment file
cp env.example .env

# Edit environment variables
nano .env
```

### 2. Deploy Complete Application
```bash
# Deploy everything
./scripts/deploy-full.sh
```

**URLs:**
- Frontend: http://localhost
- Backend API: http://localhost:8000
- Health Check: http://localhost/health

### 3. Deploy Services Independently
```bash
# Deploy backend only
./scripts/deploy-backend.sh

# Deploy frontend only
cd React-Bots
./scripts/deploy-frontend.sh
```

## 🔧 Configuration

### Environment Variables

**Backend (.env in bots_rpa/):**
```bash
# Database
POSTGRES_PASSWORD=your_secure_password_here
DATABASE_URL=postgresql://rpa_user:your_secure_password_here@postgres:5432/rpa_bots

# Redis
REDIS_URL=redis://redis:6379/0
CELERY_BROKER_URL=redis://redis:6379/1
CELERY_RESULT_BACKEND=redis://redis:6379/1

# Security
SECRET_KEY=your-super-secret-key-change-this-in-production
ENVIRONMENT=production
```

**Frontend (.env in React-Bots/):**
```bash
# Frontend Configuration
NODE_ENV=production
VITE_API_URL=http://localhost:8000
```

## 📊 Monitoring & Health Checks

### Health Check Endpoints
- **Backend**: `http://localhost:8000/health`
- **Frontend**: `http://localhost/health`
- **Complete App**: `http://localhost/health`

### Service Logs
```bash
# All services
docker-compose -f docker-compose.full.yml logs -f

# Backend only
docker-compose logs -f

# Frontend only
cd React-Bots
docker-compose logs -f
```

## 🔄 Scaling Strategies

### 1. Independent Scaling
```bash
# Scale backend workers
docker-compose up -d --scale backend=3

# Scale frontend instances
cd React-Bots
docker-compose up -d --scale frontend=2
```

### 2. Load Balancer Configuration
```yaml
# docker-compose.loadbalancer.yml
services:
  nginx-lb:
    image: nginx:alpine
    ports:
      - "80:80"
    volumes:
      - ./docker/nginx/loadbalancer.conf:/etc/nginx/nginx.conf
    depends_on:
      - backend
      - frontend
```

### 3. Kubernetes Deployment
```yaml
# k8s/backend-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rpa-backend
spec:
  replicas: 3
  selector:
    matchLabels:
      app: rpa-backend
  template:
    metadata:
      labels:
        app: rpa-backend
    spec:
      containers:
      - name: backend
        image: rpa-backend:latest
        ports:
        - containerPort: 8000
```

## 🚀 Deployment Options

### 1. On-Premise Server
```bash
# Install Docker
curl -fsSL https://get.docker.com | sh

# Deploy complete application
git clone <your-repo>
cd bots_rpa
./scripts/deploy-full.sh
```

### 2. Cloud Deployment

#### AWS ECS/Fargate
```yaml
# aws/task-definition.json
{
  "family": "rpa-bots",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "2048",
  "containerDefinitions": [
    {
      "name": "backend",
      "image": "rpa-backend:latest",
      "portMappings": [{"containerPort": 8000}]
    }
  ]
}
```

#### Google Cloud Run
```bash
# Deploy backend
gcloud run deploy rpa-backend \
  --image gcr.io/PROJECT_ID/rpa-backend \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated

# Deploy frontend
gcloud run deploy rpa-frontend \
  --image gcr.io/PROJECT_ID/rpa-frontend \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated
```

#### Azure Container Instances
```bash
# Deploy backend
az container create \
  --resource-group myResourceGroup \
  --name rpa-backend \
  --image rpa-backend:latest \
  --ports 8000

# Deploy frontend
az container create \
  --resource-group myResourceGroup \
  --name rpa-frontend \
  --image rpa-frontend:latest \
  --ports 80
```

## 🔒 Security Best Practices

### 1. Network Isolation
```yaml
# Separate networks for different environments
networks:
  backend_network:
    driver: bridge
  frontend_network:
    driver: bridge
  database_network:
    driver: bridge
```

### 2. Secrets Management
```bash
# Use Docker secrets in production
echo "your-secret" | docker secret create db_password -

# Reference in docker-compose.yml
secrets:
  - db_password
```

### 3. Container Security
```dockerfile
# Run as non-root user
USER appuser

# Health checks
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1
```

## 📈 Performance Optimization

### 1. Multi-stage Builds
```dockerfile
# Backend Dockerfile
FROM python:3.10-slim as builder
# Build dependencies

FROM python:3.10-slim
# Copy from builder
```

### 2. Caching Strategies
```yaml
# Frontend with build cache
services:
  frontend:
    build:
      context: .
      cache_from:
        - rpa-frontend:latest
```

### 3. Resource Limits
```yaml
# Set resource limits
deploy:
  resources:
    limits:
      memory: 1G
      cpus: '0.5'
    reservations:
      memory: 512M
      cpus: '0.25'
```

## 🛠️ Troubleshooting

### Common Issues

#### 1. Service Communication
```bash
# Check network connectivity
docker network ls
docker network inspect rpa_network

# Test service communication
docker-compose exec backend curl frontend:80
```

#### 2. Port Conflicts
```bash
# Check port usage
sudo netstat -tulpn | grep :8000

# Change ports in docker-compose.yml
ports:
  - "8001:8000"
```

#### 3. Memory Issues
```bash
# Monitor resource usage
docker stats

# Increase memory limits
deploy:
  resources:
    limits:
      memory: 2G
```

### Debug Commands
```bash
# Enter containers
docker-compose exec backend bash
docker-compose exec frontend sh

# View logs
docker-compose logs -f backend
docker-compose logs -f frontend

# Restart services
docker-compose restart backend
docker-compose restart frontend
```

## 📚 Additional Resources

- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [AWS ECS Documentation](https://docs.aws.amazon.com/ecs/)
- [Google Cloud Run Documentation](https://cloud.google.com/run/docs)
- [Azure Container Instances Documentation](https://docs.microsoft.com/en-us/azure/container-instances/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with Docker
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details. 