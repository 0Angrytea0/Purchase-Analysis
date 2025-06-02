from .customers import router as customers_router
from .products import router as products_router
from .purchases import router as purchases_router
from .sellers import router as sellers_router

__all__ = ["customers_router", "products_router", "purchases_router", "sellers_router"]
