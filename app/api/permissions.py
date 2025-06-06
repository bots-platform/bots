from fastapi import APIRouter, Depends, HTTPException, status
from ..services.json_storage import json_storage
from .. import auth
from typing import List, Dict, Any

router = APIRouter(prefix="/api/permissions", tags=["permissions"])

def get_current_user(token: str = Depends(auth.oauth2_scheme)) -> Dict:
    from jose import jwt, JWTError
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

@router.post("/", status_code=201)
def create_permission(permission: Dict[str, Any], current_user: dict = Depends(get_current_user)):
    if not current_user["is_admin"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    permissions = json_storage.get_permissions()
    if any(p["name"] == permission["name"] for p in permissions):
        raise HTTPException(status_code=400, detail="Permission already exists")
    permission_id = max([p.get("id", 0) for p in permissions], default=0) + 1
    permission_data = {
        "id": permission_id,
        "name": permission["name"],
        "description": permission.get("description", "")
    }
    permissions.append(permission_data)
    json_storage._write_json(json_storage.permissions_file, {"permissions": permissions})
    return permission_data

@router.get("/", response_model=List[Dict])
def read_permissions(current_user: dict = Depends(get_current_user)):
    if not current_user["is_admin"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    return json_storage.get_permissions() 