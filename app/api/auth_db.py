from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from datetime import timedelta
from typing import List, Dict, Any
from jose import jwt, JWTError

from ..services.database_service import DatabaseService
from .. import auth

router = APIRouter(prefix="/api/auth", tags=["auth"])

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
    
    # Crear token de acceso
    access_token_expires = timedelta(minutes=auth.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = auth.create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    
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
async def logout(current_user: Dict = Depends(get_current_user)):
    """Endpoint de logout (el token se invalida en el frontend)"""
    return {"message": "Successfully logged out"}

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