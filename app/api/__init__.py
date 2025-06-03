from .sga import router as sga_router
from .oplogin import router as oplogin_router
from .newCallCenter import router as newCallCenter_router
from .semaforo import router as semaforo_router
from .reporteCombinado import router as reporteCombinado_router
from .sharepoint_horario_general_atcorp import router as sharepoint_horario_general_router
from .sharepoint_Horario_mesa_atcorp import router as sharepoint_horario_mesa_router
from .minpub import router as minpub_router
from .pronatel import router as pronatel_router
from .auth import router as auth_router
from .users import router as users_router
from .permissions import router as permissions_router

__all__ = [
    "sga_router",
    "oplogin_router",
    "newCallCenter_router",
    "semaforo_router",
    "reporteCombinado_router",
    "sharepoint_horario_general_router",
    "sharepoint_horario_mesa_router",
    "minpub_router",
    "pronatel_router",
    "auth_router",
    "users_router",
    "permissions_router"
]
