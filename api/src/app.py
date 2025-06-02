from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import logging
from fastapi.security import OAuth2PasswordRequestForm
from datetime import timedelta
from sqlalchemy import text
from sqlalchemy.orm import Session
from contextlib import contextmanager

# Настройка логгирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from .routers import products, sellers, purchases, customers
from .db import engine, Base, get_db
from .models import *
from .config import settings
from .producers import KafkaService

app = FastAPI(
    title="Shop Analytics API",
    description="API for shop analytics and data processing",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Подключение роутеров
app.include_router(products.router, prefix=settings.API_V1_PREFIX, tags=["products"])
app.include_router(sellers.router, prefix=settings.API_V1_PREFIX, tags=["sellers"])
app.include_router(purchases.router, prefix=settings.API_V1_PREFIX, tags=["purchases"])
app.include_router(customers.router, prefix=settings.API_V1_PREFIX, tags=["customers"])

@app.get("/")
def read_root():
    return {
        "status": "ok",
        "message": "Shop Analytics API is running",
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check():
    status = {
        "status": "unhealthy",
        "database": "disconnected",
        "kafka": "disconnected",
        "details": {}
    }

    try:
        with contextmanager(get_db)() as db:
            db.execute(text("SELECT 1"))
            status["database"] = "connected"
    except Exception as e:
        error_msg = f"Database connection failed: {str(e)}"
        logger.error(error_msg)
        status["details"]["database_error"] = error_msg
        return status

    try:
        if await KafkaService.health_check():
            status["kafka"] = "connected"
        else:
            status["details"]["kafka_error"] = "Kafka not available"
    except Exception as e:
        error_msg = f"Kafka connection failed: {str(e)}"
        logger.error(error_msg)
        status["details"]["kafka_error"] = error_msg

    if status["database"] == "connected":
        status["status"] = "healthy" if status["kafka"] == "connected" else "degraded"

    return status
