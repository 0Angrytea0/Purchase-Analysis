from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
from ..db import get_db
from .. import schemas, models, producers
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/clients", tags=["clients"])

@router.post("/", response_model=schemas.ClientOut)
async def create_client(
    client: schemas.ClientCreate,
    db: Session = Depends(get_db)
):
    """Создает нового клиента"""
    # Проверяем уникальность email
    existing_client = db.query(models.Client).filter(models.Client.email == client.email).first()
    if existing_client:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )

    db_client = models.Client(
        first_name=client.first_name,
        last_name=client.last_name,
        email=client.email
    )

    db.add(db_client)
    db.commit()
    db.refresh(db_client)

    try:
        client_data = {
            "client_id": db_client.client_id,
            "first_name": db_client.first_name,
            "last_name": db_client.last_name,
            "email": db_client.email,
            "signup_date": db_client.signup_date.isoformat()
        }
        await producers.send_client(client_data)
        logger.info(f"Client {db_client.client_id} created and sent to Kafka")
    except Exception as e:
        logger.error(f"Failed to send client to Kafka: {str(e)}")

    return db_client

@router.get("/", response_model=List[schemas.ClientOut])
async def list_clients(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """Возвращает список клиентов с пагинацией"""
    clients = db.query(models.Client).offset(skip).limit(limit).all()
    return clients

@router.get("/{client_id}", response_model=schemas.ClientOut)
async def get_client(
    client_id: int,
    db: Session = Depends(get_db)
):
    """Возвращает клиента по ID"""
    client = db.query(models.Client).filter(models.Client.client_id == client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    return client

@router.put("/{client_id}", response_model=schemas.ClientOut)
async def update_client(
    client_id: int,
    client_data: schemas.ClientUpdate,
    db: Session = Depends(get_db)
):
    """Обновляет данные клиента"""
    db_client = db.query(models.Client).filter(models.Client.client_id == client_id).first()
    if not db_client:
        raise HTTPException(status_code=404, detail="Client not found")

    update_data = client_data.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_client, key, value)

    db.commit()
    db.refresh(db_client)

    try:
        client_update = {
            "client_id": db_client.client_id,
            "first_name": db_client.first_name,
            "last_name": db_client.last_name,
            "email": db_client.email,
            "operation": "update"
        }
        await producers.send_client(client_update)
        logger.info(f"Client {db_client.client_id} update sent to Kafka")
    except Exception as e:
        logger.error(f"Failed to send client update to Kafka: {str(e)}")

    return db_client

@router.delete("/{client_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_client(
    client_id: int,
    db: Session = Depends(get_db)
):
    """Удаляет клиента"""
    db_client = db.query(models.Client).filter(models.Client.client_id == client_id).first()
    if not db_client:
        raise HTTPException(status_code=404, detail="Client not found")

    # Проверяем есть ли связанные покупки
    purchases = db.query(models.Purchase).filter(models.Purchase.client_id == client_id).count()
    if purchases > 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete client with existing purchases"
        )

    try:
        delete_data = {
            "client_id": client_id,
            "operation": "delete"
        }
        await producers.send_client(delete_data)
        logger.info(f"Client {client_id} deletion sent to Kafka")
    except Exception as e:
        logger.error(f"Failed to send client deletion to Kafka: {str(e)}")

    db.delete(db_client)
    db.commit()
    return None
