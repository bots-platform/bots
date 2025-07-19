from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv
from .database import Base, engine
from .user import User
from .permission import Permission
from .user_permission import UserPermission
from .. import auth

load_dotenv()

def create_tables():
    """Crea todas las tablas en la base de datos"""
    Base.metadata.create_all(bind=engine)
    print("✅ Tablas creadas exitosamente")

def seed_initial_data():
    """Crea datos iniciales: permisos y usuario admin"""
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    
    try:
        # Crear permisos iniciales
        permissions_data = [
            {"name": "admin", "description": "Acceso total al sistema"},
            {"name": "upload", "description": "Acceso al validador Minpub"},
            {"name": "sga_report", "description": "Acceso a reportes SGA"},
            {"name": "validator", "description": "Acceso al validador Minpub V2"},
            {"name": "user_management", "description": "Gestión de usuarios"},
            {"name": "permission_management", "description": "Gestión de permisos"},
            {"name": "system_logs", "description": "Acceso a logs del sistema"},
            {"name": "reports", "description": "Acceso a reportes generales"}
        ]
        
        for perm_data in permissions_data:
            permission = Permission.get_or_create(db, perm_data["name"], perm_data["description"])
            print(f"✅ Permiso creado: {permission.name}")
        
        # Crear usuario admin si no existe
        admin_user = db.query(User).filter(User.username == "admin").first()
        if not admin_user:
            admin_password = os.getenv("ADMIN_PASSWORD", "losmelones")
            hashed_password = auth.get_password_hash(admin_password)
            
            admin_user = User(
                username="admin",
                email="admin@rpa-bots.com",
                hashed_password=hashed_password,
                is_active=True,
                is_admin=True,
                full_name="Administrador del Sistema"
            )
            db.add(admin_user)
            db.commit()
            db.refresh(admin_user)
            
            # Asignar todos los permisos al admin
            for permission in db.query(Permission).all():
                admin_user.add_permission(permission.name, db)
            
            print("✅ Usuario admin creado exitosamente")
            print(f"   Username: admin")
            print(f"   Password: {admin_password}")
            print("   ⚠️  Cambia la contraseña después del primer login!")
        
        print("✅ Datos iniciales creados exitosamente")
        
    except Exception as e:
        print(f"❌ Error creando datos iniciales: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def migrate_from_json():
    """Migra datos desde archivos JSON a PostgreSQL"""
    from ..services.json_storage import json_storage
    import json
    from pathlib import Path
    
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    
    try:
        # Migrar usuarios desde JSON
        users_data = json_storage.get_users()
        
        for user_data in users_data:
            # Verificar si el usuario ya existe
            existing_user = db.query(User).filter(User.username == user_data["username"]).first()
            if existing_user:
                print(f"⚠️  Usuario ya existe: {user_data['username']}")
                continue
            
            # Crear nuevo usuario
            new_user = User(
                username=user_data["username"],
                email=user_data.get("email", f"{user_data['username']}@rpa-bots.com"),
                hashed_password=user_data["hashed_password"],
                is_active=user_data.get("is_active", True),
                is_admin=user_data.get("is_admin", False),
                full_name=user_data.get("full_name", user_data["username"])
            )
            db.add(new_user)
            db.commit()
            db.refresh(new_user)
            
            # Migrar permisos del usuario
            if "permissions" in user_data:
                for perm_name in user_data["permissions"]:
                    try:
                        new_user.add_permission(perm_name, db)
                    except ValueError as e:
                        print(f"⚠️  Permiso no encontrado: {perm_name}")
            
            print(f"✅ Usuario migrado: {user_data['username']}")
        
        print("✅ Migración desde JSON completada")
        
    except Exception as e:
        print(f"❌ Error en migración: {e}")
        db.rollback()
        raise
    finally:
        db.close()

def run_migration():
    """Ejecuta la migración completa"""
    print("🚀 Iniciando migración de base de datos...")
    
    # Crear tablas
    create_tables()
    
    # Crear datos iniciales
    seed_initial_data()
    
    # Migrar datos existentes (opcional)
    try:
        migrate_from_json()
    except Exception as e:
        print(f"⚠️  Migración desde JSON falló: {e}")
    
    print("🎉 Migración completada exitosamente!")

if __name__ == "__main__":
    run_migration() 