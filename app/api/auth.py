from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from datetime import timedelta
from typing import List

from ..services.json_storage import json_storage
from .. import auth

router = APIRouter(prefix="/api/auth", tags=["auth"])

@router.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = json_storage.get_user_by_username(form_data.username)
    print(f"Intento de login: usuario={form_data.username}, password={form_data.password}")
    if user:
        print(f"Hash almacenado: {user['hashed_password']}")
        try:
            verificado = auth.verify_password(form_data.password, user["hashed_password"])
        except Exception as e:
            print(f"Error al verificar password: {e}")
            verificado = False
        print(f"¿Password verificado?: {verificado}")
    else:
        print("Usuario no encontrado en JSON")
    if not user or not auth.verify_password(form_data.password, user["hashed_password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token_expires = timedelta(minutes=auth.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = auth.create_access_token(
        data={"sub": user["username"]}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}