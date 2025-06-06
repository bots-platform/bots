from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from ..services.json_storage import json_storage
from .. import auth
from typing import List, Dict, Any
from jose import jwt, JWTError

router = APIRouter(prefix="/api/users", tags=["users"])

oauth2_scheme = auth.oauth2_scheme

# Utilidad para obtener el usuario actual desde el token

def get_current_user(token: str = Depends(oauth2_scheme)) -> Dict:
    try:
        payload = jwt.decode(token, auth.SECRET_KEY, algorithms=[auth.ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    user = json_storage.get_user_by_username(username)
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
    return user

@router.get("/me")
async def read_users_me(current_user: dict = Depends(get_current_user)):
    return {
        "id": current_user["id"],
        "username": current_user["username"],
        "email": current_user["email"],
        "is_admin": current_user["is_admin"],
        "permissions": current_user.get("permissions", [])
    }

@router.post("/", status_code=201)
def create_user(user: Dict[str, Any]):
    if json_storage.get_user_by_username(user["username"]):
        raise HTTPException(status_code=400, detail="Username already registered")
    hashed_password = auth.get_password_hash(user["password"])
    user_data = {
        "username": user["username"],
        "email": user.get("email", ""),
        "hashed_password": hashed_password,
        "is_active": True,
        "is_admin": user.get("is_admin", False),
        "permissions": user.get("permissions", [])
    }
    created_user = json_storage.create_user(user_data)
    return {k: v for k, v in created_user.items() if k != "hashed_password"}

@router.get("/", response_model=List[Dict])
def read_users(current_user: dict = Depends(get_current_user)):
    if not current_user["is_admin"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    users = json_storage.get_users()
    # No exponer hashed_password
    return [{k: v for k, v in user.items() if k != "hashed_password"} for user in users]

@router.put("/{user_id}")
def update_user(user_id: int, user_update: Dict[str, Any], current_user: dict = Depends(get_current_user)):
    if not current_user["is_admin"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    update_data = user_update.copy()
    if "password" in update_data:
        update_data["hashed_password"] = auth.get_password_hash(update_data.pop("password"))
    updated_user = json_storage.update_user(user_id, update_data)
    if not updated_user:
        raise HTTPException(status_code=404, detail="User not found")
    return {k: v for k, v in updated_user.items() if k != "hashed_password"} 