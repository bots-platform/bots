from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base

class Permission(Base):
    __tablename__ = "permissions"
    
    # Campos principales
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), unique=True, index=True, nullable=False)
    description = Column(Text, nullable=True)
    
    # Campos de estado
    is_active = Column(Boolean, default=True, nullable=False)
    
    # Campos de auditoría
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    # Relaciones - Simplificada para evitar conflictos
    user_permissions = relationship("UserPermission", back_populates="permission", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Permission(id={self.id}, name='{self.name}', description='{self.description}')>"
    
    @classmethod
    def get_or_create(cls, db_session, name: str, description: str = None):
        """Obtiene un permiso o lo crea si no existe"""
        permission = db_session.query(cls).filter(cls.name == name).first()
        if not permission:
            permission = cls(name=name, description=description)
            db_session.add(permission)
            db_session.commit()
            db_session.refresh(permission)
        return permission 