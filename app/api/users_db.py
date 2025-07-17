from fastapi import APIRouter, Depends, HTTPException, status
from typing import List, Dict, Any, Optional
from pydantic import BaseModel

from ..services.database_service import DatabaseService
from .auth_db import get_current_user

router = APIRouter(prefix="/api/users", tags=["users"])

# Pydantic models para validación
class UserCreate(BaseModel):
    username: str
    email: str
    password: str
    full_name: str = None
    phone: str = None
    department: str = None
    is_admin: bool = False
    permissions: List[str] = []

class UserUpdate(BaseModel):
    email: str = None
    full_name: str = None
    phone: str = None
    department: str = None
    is_admin: bool = None
    is_active: bool = None
    permissions: List[str] = None
    password: str = None

class UserResponse(BaseModel):
    id: int
    username: str
    email: str
    full_name: Optional[str] = None
    phone: Optional[str] = None
    department: Optional[str] = None
    is_admin: bool
    is_active: bool
    permissions: List[str]
    created_at: str
    updated_at: str
    last_login: Optional[str] = None

@router.get("/me", response_model=UserResponse)
async def read_current_user(
    current_user: Dict = Depends(get_current_user),
    db_service: DatabaseService = Depends()
):
    """Obtiene el usuario actual autenticado"""
    user = db_service.get_user_by_id(current_user["id"])
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return UserResponse(
        id=user.id,
        username=user.username,
        email=user.email,
        full_name=user.full_name,
        phone=user.phone,
        department=user.department,
        is_admin=user.is_admin,
        is_active=user.is_active,
        permissions=user.permission_names,
        created_at=user.created_at.isoformat(),
        updated_at=user.updated_at.isoformat(),
        last_login=user.last_login.isoformat() if user.last_login else None
    )

@router.get("/", response_model=List[UserResponse])
async def read_users(
    current_user: Dict = Depends(get_current_user),
    db_service: DatabaseService = Depends()
):
    """Obtiene todos los usuarios (solo admin)"""
    if not current_user["is_admin"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    users = db_service.get_users()
    return [
        UserResponse(
            id=user.id,
            username=user.username,
            email=user.email,
            full_name=user.full_name,
            phone=user.phone,
            department=user.department,
            is_admin=user.is_admin,
            is_active=user.is_active,
            permissions=user.permission_names,
            created_at=user.created_at.isoformat(),
            updated_at=user.updated_at.isoformat(),
            last_login=user.last_login.isoformat() if user.last_login else None
        )
        for user in users
    ]

@router.get("/{user_id}", response_model=UserResponse)
async def read_user(
    user_id: int,
    current_user: Dict = Depends(get_current_user),
    db_service: DatabaseService = Depends()
):
    """Obtiene un usuario específico"""
    if not current_user["is_admin"] and current_user["id"] != user_id:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    user = db_service.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return UserResponse(
        id=user.id,
        username=user.username,
        email=user.email,
        full_name=user.full_name,
        phone=user.phone,
        department=user.department,
        is_admin=user.is_admin,
        is_active=user.is_active,
        permissions=user.permission_names,
        created_at=user.created_at.isoformat(),
        updated_at=user.updated_at.isoformat(),
        last_login=user.last_login.isoformat() if user.last_login else None
    )

@router.post("/", response_model=UserResponse, status_code=201)
async def create_user(
    user_data: UserCreate,
    current_user: Dict = Depends(get_current_user),
    db_service: DatabaseService = Depends()
):
    """Crea un nuevo usuario (solo admin)"""
    if not current_user["is_admin"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    try:
        user = db_service.create_user(user_data.dict())
        return UserResponse(
            id=user.id,
            username=user.username,
            email=user.email,
            full_name=user.full_name,
            phone=user.phone,
            department=user.department,
            is_admin=user.is_admin,
            is_active=user.is_active,
            permissions=user.permission_names,
            created_at=user.created_at.isoformat(),
            updated_at=user.updated_at.isoformat(),
            last_login=user.last_login.isoformat() if user.last_login else None
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating user: {str(e)}")

@router.put("/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: int,
    user_data: UserUpdate,
    current_user: Dict = Depends(get_current_user),
    db_service: DatabaseService = Depends()
):
    """Actualiza un usuario (solo admin o el propio usuario)"""
    if not current_user["is_admin"] and current_user["id"] != user_id:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Solo admin puede cambiar is_admin, is_active y permisos
    if not current_user["is_admin"]:
        user_data.is_admin = None
        user_data.is_active = None
        user_data.permissions = None
    
    user = db_service.update_user(user_id, user_data.dict(exclude_unset=True))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return UserResponse(
        id=user.id,
        username=user.username,
        email=user.email,
        full_name=user.full_name,
        phone=user.phone,
        department=user.department,
        is_admin=user.is_admin,
        is_active=user.is_active,
        permissions=user.permission_names,
        created_at=user.created_at.isoformat(),
        updated_at=user.updated_at.isoformat(),
        last_login=user.last_login.isoformat() if user.last_login else None
    )

@router.delete("/{user_id}")
async def delete_user(
    user_id: int,
    current_user: Dict = Depends(get_current_user),
    db_service: DatabaseService = Depends()
):
    """Elimina un usuario (soft delete, solo admin)"""
    if not current_user["is_admin"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    if current_user["id"] == user_id:
        raise HTTPException(status_code=400, detail="Cannot delete yourself")
    
    success = db_service.delete_user(user_id)
    if not success:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"message": "User deleted successfully"}

@router.post("/{user_id}/permissions")
async def add_user_permission(
    user_id: int,
    permission_name: str,
    current_user: Dict = Depends(get_current_user),
    db_service: DatabaseService = Depends()
):
    """Agrega un permiso a un usuario (solo admin)"""
    if not current_user["is_admin"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    user = db_service.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    try:
        user.add_permission(permission_name, db_service.db)
        return {"message": f"Permission '{permission_name}' added successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.delete("/{user_id}/permissions/{permission_name}")
async def remove_user_permission(
    user_id: int,
    permission_name: str,
    current_user: Dict = Depends(get_current_user),
    db_service: DatabaseService = Depends()
):
    """Remueve un permiso de un usuario (solo admin)"""
    if not current_user["is_admin"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    user = db_service.get_user_by_id(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    try:
        user.remove_permission(permission_name, db_service.db)
        return {"message": f"Permission '{permission_name}' removed successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) 