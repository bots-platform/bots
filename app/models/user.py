from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base
import uuid

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, index=True, nullable=False)
    email = Column(String(100), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    
    is_active = Column(Boolean, default=True, nullable=False)
    is_admin = Column(Boolean, default=False, nullable=False)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    last_login = Column(DateTime(timezone=True), nullable=True)
    
    full_name = Column(String(100), nullable=True)
    phone = Column(String(20), nullable=True)
    department = Column(String(50), nullable=True)
    
    user_permissions = relationship("UserPermission", back_populates="user", foreign_keys="UserPermission.user_id", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}', email='{self.email}')>"
    
    @property
    def permission_names(self):
        """Retorna lista de nombres de permisos del usuario"""
        return [up.permission.name for up in self.user_permissions]
    
    def has_permission(self, permission_name: str) -> bool:
        """Verifica si el usuario tiene un permiso específico"""
        if self.is_admin:
            return True
        return any(up.permission.name == permission_name for up in self.user_permissions)
    
    def add_permission(self, permission_name: str, db_session):
        """Agrega un permiso al usuario"""
        from .permission import Permission
        from .user_permission import UserPermission
        
        permission = db_session.query(Permission).filter(Permission.name == permission_name).first()
        if not permission:
            raise ValueError(f"Permission '{permission_name}' not found")
        
        existing = db_session.query(UserPermission).filter(
            UserPermission.user_id == self.id,
            UserPermission.permission_id == permission.id
        ).first()
        
        if not existing:
            user_permission = UserPermission(user_id=self.id, permission_id=permission.id)
            db_session.add(user_permission)
            db_session.commit()
    
    def remove_permission(self, permission_name: str, db_session):
        """Remueve un permiso del usuario"""
        from .permission import Permission
        from .user_permission import UserPermission
        
        permission = db_session.query(Permission).filter(Permission.name == permission_name).first()
        if permission:
            user_permission = db_session.query(UserPermission).filter(
                UserPermission.user_id == self.id,
                UserPermission.permission_id == permission.id
            ).first()
            
            if user_permission:
                db_session.delete(user_permission)
                db_session.commit() 