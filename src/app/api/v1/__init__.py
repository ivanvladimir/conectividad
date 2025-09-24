from fastapi import APIRouter

from .login import router as login_router
from .logout import router as logout_router
from .utils import router as utils_router
from .rate_limits import router as rate_limits_router
from .tasks import router as tasks_router

router = APIRouter(prefix="/v1")
router.include_router(login_router)
router.include_router(logout_router)
router.include_router(utils_router)
router.include_router(tasks_router)
router.include_router(rate_limits_router)
