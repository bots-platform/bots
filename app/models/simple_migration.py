from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://rpa_user:admin@localhost:5432/rpa_bots")

engine = create_engine(DATABASE_URL)

def create_tables():
    """Crea las tablas usando SQL directo"""
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                email VARCHAR(100) UNIQUE NOT NULL,
                hashed_password VARCHAR(255) NOT NULL,
                is_active BOOLEAN DEFAULT TRUE NOT NULL,
                is_admin BOOLEAN DEFAULT FALSE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
                last_login TIMESTAMP WITH TIME ZONE,
                full_name VARCHAR(100),
                phone VARCHAR(20),
                department VARCHAR(50)
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS permissions (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) UNIQUE NOT NULL,
                description TEXT,
                is_active BOOLEAN DEFAULT TRUE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
            )
        """))
        
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS user_permissions (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                permission_id INTEGER NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
                granted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
                granted_by INTEGER REFERENCES users(id),
                UNIQUE(user_id, permission_id)
            )
        """))
        
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_permissions_name ON permissions(name)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_user_permissions_user_id ON user_permissions(user_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_user_permissions_permission_id ON user_permissions(permission_id)"))
        
        print("✅ Tablas creadas exitosamente")

def seed_initial_data():
    """Crea datos iniciales usando SQL directo"""
    with engine.connect() as conn:
        permissions_data = [
            ("admin", "Acceso total al sistema"),
            ("upload", "Acceso al validador Minpub"),
            ("sga_report", "Acceso a reportes SGA"),
            ("validator", "Acceso al validador Minpub V2"),
            ("patches", "Acceso a parches Minpub"),
            ("user_management", "Gestión de usuarios"),
            ("permission_management", "Gestión de permisos"),
            ("system_logs", "Acceso a logs del sistema"),
            ("reports", "Acceso a reportes generales")
        ]
        
        for name, description in permissions_data:
            conn.execute(text("""
                INSERT INTO permissions (name, description, is_active) 
                VALUES (:name, :description, TRUE)
                ON CONFLICT (name) DO NOTHING
            """), {"name": name, "description": description})
            print(f"✅ Permiso creado: {name}")
        
        from app import auth
        admin_password = os.getenv("ADMIN_PASSWORD", "admin123")
        hashed_password = auth.get_password_hash(admin_password)
        
        conn.execute(text("""
            INSERT INTO users (username, email, hashed_password, is_active, is_admin, full_name)
            VALUES (:username, :email, :hashed_password, :is_active, :is_admin, :full_name)
            ON CONFLICT (username) DO NOTHING
        """), {
            "username": "mozac",
            "email": "mozac@rpa-bots.com",
            "hashed_password": hashed_password,
            "is_active": True,
            "is_admin": True,
            "full_name": "Administrador del Sistema"
        })
        
        conn.execute(text("""
            INSERT INTO user_permissions (user_id, permission_id)
            SELECT u.id, p.id
            FROM users u, permissions p
            WHERE u.username = 'mozac'
            ON CONFLICT (user_id, permission_id) DO NOTHING
        """))
        
        print("✅ Usuario admin creado exitosamente")
        print(f"   Username: mozac")
        print(f"   Password: {admin_password}")
        print("   ⚠️  Cambia la contraseña después del primer login!")

def migrate_from_json():
    """Migra datos desde archivos JSON"""
    from app.services.json_storage import json_storage
    
    with engine.connect() as conn:
        users_data = json_storage.get_users()
        
        for user_data in users_data:
            result = conn.execute(text("SELECT id FROM users WHERE username = :username"), 
                                {"username": user_data["username"]})
            if result.fetchone():
                print(f"⚠️  Usuario ya existe: {user_data['username']}")
                continue
            
            conn.execute(text("""
                INSERT INTO users (username, email, hashed_password, is_active, is_admin, full_name)
                VALUES (:username, :email, :hashed_password, :is_active, :is_admin, :full_name)
            """), {
                "username": user_data["username"],
                "email": user_data.get("email", f"{user_data['username']}@rpa-bots.com"),
                "hashed_password": user_data["hashed_password"],
                "is_active": user_data.get("is_active", True),
                "is_admin": user_data.get("is_admin", False),
                "full_name": user_data.get("full_name", user_data["username"])
            })
            
            if "permissions" in user_data:
                for perm_name in user_data["permissions"]:
                    conn.execute(text("""
                        INSERT INTO user_permissions (user_id, permission_id)
                        SELECT u.id, p.id
                        FROM users u, permissions p
                        WHERE u.username = :username AND p.name = :perm_name
                        ON CONFLICT (user_id, permission_id) DO NOTHING
                    """), {"username": user_data["username"], "perm_name": perm_name})
            
            print(f"✅ Usuario migrado: {user_data['username']}")
        
        print("✅ Migración desde JSON completada")

def run_simple_migration():
    """Ejecuta la migración simplificada"""
    print("🚀 Iniciando migración simplificada...")
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ Conexión a PostgreSQL exitosa")
        
        create_tables()
        
        seed_initial_data()
        
        try:
            migrate_from_json()
        except Exception as e:
            print(f"⚠️  Migración desde JSON falló: {e}")
        
        print("🎉 Migración simplificada completada exitosamente!")
        
    except Exception as e:
        print(f"❌ Error durante la migración: {e}")
        raise

if __name__ == "__main__":
    run_simple_migration()
