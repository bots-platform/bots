"""
Script de inicialización de base de datos
Se ejecuta automáticamente al arrancar la aplicación
"""
import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

def init_database():
    """Inicializa la base de datos si es necesario"""
    try:
        DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://rpa_user:rpa_password@localhost:5432/rpa_bots")
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            # Verificar si las tablas existen
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'users'
                )
            """))
            
            if not result.fetchone()[0]:
                logger.info("Inicializando base de datos...")
                
                # Crear tablas
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
                
                # Crear índices
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_permissions_name ON permissions(name)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_user_permissions_user_id ON user_permissions(user_id)"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_user_permissions_permission_id ON user_permissions(permission_id)"))
                
                # Insertar datos iniciales
                permissions_data = [
                    ("admin", "Acceso total al sistema"),
                    ("upload", "Acceso al validador Minpub"),
                    ("sga_report", "Acceso a reportes SGA"),
                    ("validator", "Acceso al validador Minpub V2"),
                    ("user_management", "Gestión de usuarios"),
                    ("permission_management", "Gestión de permisos"),
                    ("system_logs", "Acceso a logs del sistema"),
                    ("reports", "Acceso a reportes generales")
                ]
                
                for name, description in permissions_data:
                    conn.execute(text("""
                        INSERT INTO permissions (name, description) 
                        VALUES (:name, :description)
                        ON CONFLICT (name) DO NOTHING
                    """), {"name": name, "description": description})
                
                # Crear usuario admin
                from app import auth
                admin_password = os.getenv("ADMIN_PASSWORD", "admin123")
                hashed_password = auth.get_password_hash(admin_password)
                
                conn.execute(text("""
                    INSERT INTO users (username, email, hashed_password, is_active, is_admin, full_name)
                    VALUES (:username, :email, :hashed_password, :is_active, :is_admin, :full_name)
                    ON CONFLICT (username) DO NOTHING
                """), {
                    "username": "admin",
                    "email": "admin@rpa-bots.com",
                    "hashed_password": hashed_password,
                    "is_active": True,
                    "is_admin": True,
                    "full_name": "Administrador del Sistema"
                })
                
                # Asignar permisos al admin
                conn.execute(text("""
                    INSERT INTO user_permissions (user_id, permission_id)
                    SELECT u.id, p.id
                    FROM users u, permissions p
                    WHERE u.username = 'admin'
                    ON CONFLICT (user_id, permission_id) DO NOTHING
                """))
                
                logger.info("✅ Base de datos inicializada exitosamente")
            else:
                logger.info("✅ Base de datos ya está inicializada")
                
    except Exception as e:
        logger.error(f"❌ Error inicializando base de datos: {e}")
        # No lanzar excepción para que la app pueda arrancar
        pass 