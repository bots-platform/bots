import json
import os
from typing import List, Dict, Any, Optional
from pathlib import Path

class JSONStorage:
    def __init__(self):
        self.data_dir = Path(__file__).parent.parent / "data"
        self.users_file = self.data_dir / "users.json"
        self.permissions_file = self.data_dir / "permissions.json"
        self._ensure_files_exist()

    def _ensure_files_exist(self):
        """Asegura que los archivos JSON existan con estructura inicial."""
        self.data_dir.mkdir(exist_ok=True)
        
        if not self.users_file.exists():
            self._write_json(self.users_file, {"users": []})
        
        if not self.permissions_file.exists():
            self._write_json(self.permissions_file, {"permissions": []})

    def _read_json(self, file_path: Path) -> Dict:
        """Lee un archivo JSON."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return {}

    def _write_json(self, file_path: Path, data: Dict):
        """Escribe datos en un archivo JSON."""
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"Error writing to {file_path}: {e}")

    def get_users(self) -> List[Dict]:
        """Obtiene todos los usuarios."""
        data = self._read_json(self.users_file)
        return data.get("users", [])

    def get_user_by_username(self, username: str) -> Optional[Dict]:
        """Obtiene un usuario por su nombre de usuario."""
        users = self.get_users()
        return next((user for user in users if user["username"] == username), None)

    def create_user(self, user_data: Dict) -> Dict:
        """Crea un nuevo usuario."""
        users = self.get_users()
        user_id = max([user.get("id", 0) for user in users], default=0) + 1
        user_data["id"] = user_id
        users.append(user_data)
        self._write_json(self.users_file, {"users": users})
        return user_data

    def update_user(self, user_id: int, user_data: Dict) -> Optional[Dict]:
        """Actualiza un usuario existente."""
        users = self.get_users()
        for i, user in enumerate(users):
            if user["id"] == user_id:
                users[i] = {**user, **user_data}
                self._write_json(self.users_file, {"users": users})
                return users[i]
        return None

    def get_permissions(self) -> List[Dict]:
        """Obtiene todos los permisos."""
        data = self._read_json(self.permissions_file)
        return data.get("permissions", [])

    def get_permission_by_name(self, name: str) -> Optional[Dict]:
        """Obtiene un permiso por su nombre."""
        permissions = self.get_permissions()
        return next((p for p in permissions if p["name"] == name), None)

json_storage = JSONStorage() 