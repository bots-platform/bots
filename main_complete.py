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

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Import routers using absolute imports
from app.api.auth_db import router as auth_router
from app.api.users_db import router as users_router
from app.api.permissions import router as permissions_router

app.include_router(auth_router)
app.include_router(users_router)
app.include_router(permissions_router)

@app.get("/")
async def root():
    return {"message": "Welcome to RPA Bots API"}

@app.get("/health")
async def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 