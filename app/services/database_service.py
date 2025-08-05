from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from ..models.database import get_db
from ..models.user import User
from ..models.permission import Permission
from ..models.user_permission import UserPermission
from .. import auth
from fastapi import Depends, HTTPException, status

class DatabaseService:
    """Servicio para manejar operaciones de base de datos"""
    
    def __init__(self, db: Session = Depends(get_db)):
        self.db = db
    
    def get_users(self) -> List[User]:
        """Obtiene todos los usuarios activos"""
        return self.db.query(User).filter(User.is_active == True).all()
    
    def get_user_by_id(self, user_id: int) -> Optional[User]:
        """Obtiene un usuario por ID"""
        return self.db.query(User).filter(User.id == user_id).first()
    
    def get_user_by_username(self, username: str) -> Optional[User]:
        """Obtiene un usuario por nombre de usuario"""
        return self.db.query(User).filter(User.username == username).first()
    
    def get_user_by_email(self, email: str) -> Optional[User]:
        """Obtiene un usuario por email"""
        return self.db.query(User).filter(User.email == email).first()
    
    def create_user(self, user_data: Dict[str, Any]) -> User:
        """Crea un nuevo usuario"""
        if self.get_user_by_username(user_data["username"]):
            raise HTTPException(status_code=400, detail="Username already registered")
        
        if self.get_user_by_email(user_data["email"]):
            raise HTTPException(status_code=400, detail="Email already registered")
        
        hashed_password = auth.get_password_hash(user_data["password"])
        
        user = User(
            username=user_data["username"],
            email=user_data["email"],
            hashed_password=hashed_password,
            is_active=user_data.get("is_active", True),
            is_admin=user_data.get("is_admin", False),
            full_name=user_data.get("full_name"),
            phone=user_data.get("phone"),
            department=user_data.get("department")
        )
        
        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)
        
        if "permissions" in user_data:
            for perm_name in user_data["permissions"]:
                try:
                    user.add_permission(perm_name, self.db)
                except ValueError:
                    pass
        
        return user
    
    def update_user(self, user_id: int, user_data: Dict[str, Any]) -> Optional[User]:
        """Actualiza un usuario existente"""
        user = self.get_user_by_id(user_id)
        if not user:
            return None
        
        for field, value in user_data.items():
            if field == "password" and value:
                user.hashed_password = auth.get_password_hash(value)
            elif field != "permissions" and hasattr(user, field):
                setattr(user, field, value)
        
        if "permissions" in user_data:
            self.db.query(UserPermission).filter(UserPermission.user_id == user_id).delete()
            
            for perm_name in user_data["permissions"]:
                try:
                    user.add_permission(perm_name, self.db)
                except ValueError:
                    pass
        
        self.db.commit()
        self.db.refresh(user)
        return user
    
    def delete_user(self, user_id: int) -> bool:
        """Elimina un usuario (soft delete)"""
        user = self.get_user_by_id(user_id)
        if not user:
            return False
        
        user.is_active = False
        self.db.commit()
        return True
    
    def update_last_login(self, user_id: int):
        """Actualiza la fecha del último login"""
        from sqlalchemy.sql import func
        user = self.get_user_by_id(user_id)
        if user:
            user.last_login = func.now()
            self.db.commit()
    
    def get_permissions(self) -> List[Permission]:
        """Obtiene todos los permisos activos"""
        return self.db.query(Permission).filter(Permission.is_active == True).all()
    
    def get_permission_by_id(self, permission_id: int) -> Optional[Permission]:
        """Obtiene un permiso por ID"""
        return self.db.query(Permission).filter(Permission.id == permission_id).first()
    
    def get_permission_by_name(self, name: str) -> Optional[Permission]:
        """Obtiene un permiso por nombre"""
        return self.db.query(Permission).filter(Permission.name == name).first()
    
    def create_permission(self, permission_data: Dict[str, Any]) -> Permission:
        """Crea un nuevo permiso"""
        if self.get_permission_by_name(permission_data["name"]):
            raise HTTPException(status_code=400, detail="Permission already exists")
        
        permission = Permission(
            name=permission_data["name"],
            description=permission_data.get("description", "")
        )
        
        self.db.add(permission)
        self.db.commit()
        self.db.refresh(permission)
        return permission
    
    def update_permission(self, permission_id: int, permission_data: Dict[str, Any]) -> Optional[Permission]:
        """Actualiza un permiso existente"""
        permission = self.get_permission_by_id(permission_id)
        if not permission:
            return None
        
        for field, value in permission_data.items():
            if hasattr(permission, field):
                setattr(permission, field, value)
        
        self.db.commit()
        self.db.refresh(permission)
        return permission
    
    def delete_permission(self, permission_id: int) -> bool:
        """Elimina un permiso (soft delete)"""
        permission = self.get_permission_by_id(permission_id)
        if not permission:
            return False
        
        permission.is_active = False
        self.db.commit()
        return True
    
    def authenticate_user(self, username: str, password: str) -> Optional[User]:
        """Autentica un usuario"""
        user = self.get_user_by_username(username)
        if not user or not user.is_active:
            return None
        
        if not auth.verify_password(password, user.hashed_password):
            return None
        
        self.update_last_login(user.id)
        
        return user
    
    def get_user_permissions(self, user_id: int) -> List[str]:
        """Obtiene los permisos de un usuario"""
        user = self.get_user_by_id(user_id)
        if not user:
            return []
        
        return user.permission_names
    
    def user_has_permission(self, user_id: int, permission_name: str) -> bool:
        """Verifica si un usuario tiene un permiso específico"""
        user = self.get_user_by_id(user_id)
        if not user:
            return False
        
        return user.has_permission(permission_name) 