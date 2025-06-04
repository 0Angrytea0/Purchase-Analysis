import os
from pydantic import BaseModel
from typing import Optional

class Settings(BaseModel):
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BROKER", "kafka:9092")
    KAFKA_PRODUCTS_TOPIC: str = "products-topic"
    KAFKA_PURCHASES_TOPIC: str = "purchases"
    KAFKA_SELLERS_TOPIC: str = "sellers-topic"
    KAFKA_CUSTOMERS_TOPIC: str = "customers-topic"

    API_V1_PREFIX: str = "/api/v1"
    RATE_LIMIT_DEFAULT: str = "100/minute"
    CORS_ORIGINS: list = ["*"]

settings = Settings()
