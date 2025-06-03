from sqlalchemy.orm import Session
from . import models, database, auth
import os
from dotenv import load_dotenv
load_dotenv()


def init_db():
    db = database.SessionLocal()

    admin_username = os.getenv("ADMIN_USERNAME", "admin")
    admin_email = os.getenv("ADMIN_EMAIL", "admin@example.com")
    admin_password = os.getenv("ADMIN_PASSWORD", "admin123")

    admin = db.query(models.User).filter(models.User.username == admin_username).first()
    if not admin:
        admin = models.User(
            username=admin_username,
            email=admin_email,
            hashed_password=auth.get_password_hash(admin_password),
            is_admin=True
        )

    
 
    permissions = [
        {"name": "view_users", "description": "Can view users"},
        {"name": "edit_users", "description": "Can edit users"},
        {"name": "delete_users", "description": "Can delete users"},
        {"name": "manage_permissions", "description": "Can manage permissions"}
    ]
    
    for perm_data in permissions:
        perm = db.query(models.Permission).filter(models.Permission.name == perm_data["name"]).first()
        if not perm:
            perm = models.Permission(**perm_data)
            db.add(perm)
    
    db.commit()
    db.close()

if __name__ == "__main__":
    init_db() 