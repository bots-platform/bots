from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from ..models import models, schemas, auth, database

router = APIRouter(prefix="/api/permissions", tags=["permissions"])

@router.post("/", response_model=schemas.Permission)
def create_permission(
    permission: schemas.PermissionCreate,
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(database.get_db)
):
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    db_permission = models.Permission(**permission.dict())
    db.add(db_permission)
    db.commit()
    db.refresh(db_permission)
    return db_permission

@router.get("/", response_model=List[schemas.Permission])
def read_permissions(
    current_user: models.User = Depends(auth.get_current_user),
    db: Session = Depends(database.get_db)
):
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="Not authorized")
    permissions = db.query(models.Permission).all()
    return permissions 