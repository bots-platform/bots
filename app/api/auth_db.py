from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from datetime import timedelta
from typing import List, Dict, Any
from jose import jwt, JWTError
import redis
import os

from ..services.database_service import DatabaseService
from .. import auth

router = APIRouter(prefix="/api/auth", tags=["auth"])

# Configuración de Redis para blacklist de tokens
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# Configuración de sesiones múltiples
MAX_SESSIONS_PER_USER = int(os.getenv("MAX_SESSIONS_PER_USER", 5))

def get_current_user(token: str = Depends(auth.oauth2_scheme), db_service: DatabaseService = Depends()) -> Dict:
    """Obtiene el usuario actual desde el token JWT"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        payload = jwt.decode(token, auth.SECRET_KEY, algorithms=[auth.ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    # Verificar si el token está en blacklist
    if redis_client.exists(f"blacklist:{token}"):
        raise credentials_exception
    
    user = db_service.get_user_by_username(username)
    if user is None or not user.is_active:
        raise credentials_exception
    
    return {
        "id": user.id,
        "username": user.username,
        "email": user.email,
        "is_admin": user.is_admin,
        "is_active": user.is_active,
        "full_name": user.full_name,
        "permissions": user.permission_names,
        "last_login": user.last_login,
        "created_at": user.created_at,
        "updated_at": user.updated_at
    }

@router.post("/login")
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db_service: DatabaseService = Depends()
):
    """Endpoint de login que autentica al usuario y retorna un token JWT"""
    user = db_service.authenticate_user(form_data.username, form_data.password)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Control de sesiones múltiples
    user_sessions_key = f"user_sessions:{user.username}"
    current_sessions = redis_client.scard(user_sessions_key)
    
    if current_sessions >= MAX_SESSIONS_PER_USER:
        # Invalidar la sesión más antigua
        oldest_session = redis_client.spop(user_sessions_key)
        if oldest_session:
            redis_client.setex(f"blacklist:{oldest_session}", 
                             auth.ACCESS_TOKEN_EXPIRE_MINUTES * 60, "1")
    
    # Crear token de acceso
    access_token_expires = timedelta(minutes=auth.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = auth.create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    
    # Registrar nueva sesión
    redis_client.sadd(user_sessions_key, access_token)
    redis_client.expire(user_sessions_key, auth.ACCESS_TOKEN_EXPIRE_MINUTES * 60)
    
    return {
        "access_token": access_token, 
        "token_type": "bearer",
        "user": {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "is_admin": user.is_admin,
            "permissions": user.permission_names
        }
    }

@router.post("/logout")
async def logout(
    current_user: Dict = Depends(get_current_user),
    token: str = Depends(auth.oauth2_scheme)
):
    """Endpoint de logout que invalida el token actual"""
    # Agregar token a blacklist
    redis_client.setex(f"blacklist:{token}", 
                      auth.ACCESS_TOKEN_EXPIRE_MINUTES * 60, "1")
    
    # Remover de sesiones activas
    user_sessions_key = f"user_sessions:{current_user['username']}"
    redis_client.srem(user_sessions_key, token)
    
    return {"message": "Successfully logged out"}

@router.post("/logout-all-sessions")
async def logout_all_sessions(current_user: Dict = Depends(get_current_user)):
    """Cierra todas las sesiones del usuario"""
    user_sessions_key = f"user_sessions:{current_user['username']}"
    all_tokens = redis_client.smembers(user_sessions_key)
    
    # Agregar todos los tokens a blacklist
    for token in all_tokens:
        redis_client.setex(f"blacklist:{token}", 
                          auth.ACCESS_TOKEN_EXPIRE_MINUTES * 60, "1")
    
    # Limpiar todas las sesiones
    redis_client.delete(user_sessions_key)
    
    return {"message": "All sessions logged out successfully"}

@router.get("/active-sessions")
async def get_active_sessions(current_user: Dict = Depends(get_current_user)):
    """Obtiene el número de sesiones activas del usuario"""
    user_sessions_key = f"user_sessions:{current_user['username']}"
    session_count = redis_client.scard(user_sessions_key)
    
    return {
        "active_sessions": session_count,
        "max_sessions": MAX_SESSIONS_PER_USER
    }

@router.get("/me")
async def read_users_me(current_user: Dict = Depends(get_current_user)):
    """Obtiene información del usuario actual"""
    return current_user

@router.post("/change-password")
async def change_password(
    current_password: str,
    new_password: str,
    current_user: Dict = Depends(get_current_user),
    db_service: DatabaseService = Depends()
):
    """Cambia la contraseña del usuario actual"""
    user = db_service.get_user_by_id(current_user["id"])
    
    if not auth.verify_password(current_password, user.hashed_password):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    
    # Actualizar contraseña
    user.hashed_password = auth.get_password_hash(new_password)
    db_service.db.commit()
    
    return {"message": "Password changed successfully"}

@router.get("/permissions")
async def get_user_permissions(current_user: Dict = Depends(get_current_user)):
    """Obtiene los permisos del usuario actual"""
    return {
        "permissions": current_user["permissions"],
        "is_admin": current_user["is_admin"]
    } 