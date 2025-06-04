import os
import json
import logging
from .config import settings
from aiokafka import AIOKafkaProducer
import asyncio

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class KafkaService:
    _producer = None

    @classmethod
    async def get_producer(cls):
        if cls._producer is None:
            try:
                cls._producer = AIOKafkaProducer(
                    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                await cls._producer.start()
                logger.info("Connected to Kafka")
            except Exception as e:
                logger.error(f"Kafka connection error: {str(e)}")
                cls._producer = None
        return cls._producer

    @classmethod
    async def close(cls):
        if cls._producer:
            await cls._producer.stop()
            cls._producer = None

    @classmethod
    async def health_check(cls):
        try:
            producer = await cls.get_producer()
            if producer:
                # Простая проверка доступности брокеров
                brokers = producer.client.cluster.brokers()
                return len(brokers) > 0
            return False
        except Exception:
            return False

async def send_product(product_data: dict):
    producer = await KafkaService.get_producer()
    try:
        if producer:
            producer.send(settings.KAFKA_PRODUCTS_TOPIC, value=product_data)
            logger.info(f"Sent product data to Kafka: {product_data.get('id', 'new')}")
        else:
            logger.warning(f"Kafka not available, product data not sent: {product_data.get('id', 'new')}")
    except Exception as e:
        logger.error(f"Failed to send product data: {str(e)}")

async def send_seller(seller_data: dict):
    producer = await KafkaService.get_producer()
    try:
        if producer:
            producer.send(settings.KAFKA_SELLERS_TOPIC, value=seller_data)
            logger.info(f"Sent seller data to Kafka: {seller_data.get('id', 'new')}")
        else:
            logger.warning(f"Kafka not available, seller data not sent: {seller_data.get('id', 'new')}")
    except Exception as e:
        logger.error(f"Failed to send seller data: {str(e)}")

async def send_purchase(purchase_data: dict):
    producer = await KafkaService.get_producer()
    if not producer:
        logger.warning(f"Kafka не доступен, покупка не отправлена: {purchase_data.get('purchase_id')}")
        return

    try:
        #  ► Ждём, пока сообщение действительно уйдёт в Kafka
        await producer.send_and_wait(settings.KAFKA_PURCHASES_TOPIC, purchase_data)
        logger.info(f"Сообщение покупки отправлено в Kafka: {purchase_data}")
    except KafkaError as e:
        logger.error(f"Не удалось отправить покупку в Kafka: {e}")

async def send_customer(customer_data: dict):
    producer = await KafkaService.get_producer()
    try:
        if producer:
            producer.send(settings.KAFKA_CUSTOMERS_TOPIC, value=customer_data)
            logger.info(f"Sent customer data to Kafka: {customer_data.get('customer_id', 'new')}")
        else:
            logger.warning(f"Kafka not available, customer data not sent: {customer_data.get('customer_id', 'new')}")
    except Exception as e:
        logger.error(f"Failed to send customer data: {str(e)}")
