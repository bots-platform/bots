from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.models.database import engine, Base

# Create database tables
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="RPA Bots API",
    description="API for RPA Bots Management System",
    version="1.0.0"
)

# Configure CORS - MUST be before routers
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://10.200.90.94:8080",
        "http://localhost:8080", 
        "http://127.0.0.1:8080",
        "http://10.200.90.94:3000",
        "http://localhost:3000",
        "*"
    ],  # Specific origins
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"],
    allow_headers=["*", "Content-Type", "Authorization", "Accept"],
    expose_headers=["*"],
    max_age=600
)

# Import routers using absolute imports
from app.api.auth_db import router as auth_router
from app.api.users_db import router as users_router
from app.api.permissions import router as permissions_router
from app.api.minpub import router as minpub_router

app.include_router(auth_router)
app.include_router(users_router)
app.include_router(permissions_router)
app.include_router(minpub_router)

@app.get("/")
async def root():
    return {"message": "Welcome to RPA Bots API"}

@app.get("/health")
async def health():
    return {"status": "healthy", "message": "Container mode - Hot reload working!", "user": "cesar"}

@app.options("/api/minpub/process-manual/")
async def options_minpub():
    """Debug endpoint for CORS preflight requests"""
    return {"message": "CORS preflight handled"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)