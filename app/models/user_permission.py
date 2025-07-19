from sqlalchemy import Column, Integer, ForeignKey, DateTime, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base

class UserPermission(Base):
    __tablename__ = "user_permissions"
    
    # Campos principales
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    permission_id = Column(Integer, ForeignKey("permissions.id", ondelete="CASCADE"), nullable=False)
    
    # Campos de auditoría
    granted_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    granted_by = Column(Integer, ForeignKey("users.id"), nullable=True)  # Quién otorgó el permiso
    
    # Relaciones - Simplificada para evitar conflictos
    user = relationship("User", back_populates="user_permissions", foreign_keys=[user_id])
    permission = relationship("Permission", back_populates="user_permissions")
    
    def __repr__(self):
        return f"<UserPermission(user_id={self.user_id}, permission_id={self.permission_id})>" 