# Only import authentication routers to avoid missing modules
from .auth_db import router as auth_router
from .users_db import router as users_router
from .permissions import router as permissions_router

__all__ = [
    "auth_router",
    "users_router",
    "permissions_router"
]
