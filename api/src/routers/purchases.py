from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
from ..db import get_db
from .. import schemas, models, producers
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/purchases", tags=["purchases"])

@router.post("/", response_model=schemas.PurchaseOut)
async def create_purchase(
    purchase: schemas.PurchaseCreate,
    db: Session = Depends(get_db)
):
    # Проверяем существование клиента и продукта
    client = db.query(models.Client).filter(models.Client.client_id == purchase.client_id).first()
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    product = db.query(models.Product).filter(models.Product.product_id == purchase.product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    # Проверяем доступное количество
    if product.quantity < purchase.quantity:
        raise HTTPException(status_code=400, detail="Not enough quantity available")

    # Создаем покупку
    db_purchase = models.Purchase(
        client_id=purchase.client_id,
        product_id=purchase.product_id,
        quantity=purchase.quantity
    )

    # Уменьшаем количество товара
    product.quantity -= purchase.quantity

    db.add(db_purchase)
    db.commit()
    db.refresh(db_purchase)

    try:
        # Берём цену из модели Product
        db_product = db.query(models.Product).filter(models.Product.product_id == db_purchase.product_id).first()
        price = float(db_product.price) if db_product else 0.0
        full_price = price * db_purchase.quantity

        # Отправляем данные о покупке вместе с price и full_price
        purchase_data = {
            "purchase_id": db_purchase.purchase_id,
            "client_id":    db_purchase.client_id,
            "product_id":   db_purchase.product_id,
            "quantity":     db_purchase.quantity,
            "price":        price,
            "full_price":   full_price,
            "purchased_at": db_purchase.purchased_at.isoformat()
        }
        await producers.send_purchase(purchase_data)
        logger.info(f"Purchase {db_purchase.purchase_id} sent to Kafka: {purchase_data}")
    except Exception as e:
        logger.error(f"Failed to send purchase to Kafka: {str(e)}")
    return db_purchase

@router.get("/{purchase_id}", response_model=schemas.PurchaseOut)
async def get_purchase(
    purchase_id: int,
    db: Session = Depends(get_db)
):
    db_purchase = db.query(models.Purchase).filter(models.Purchase.purchase_id == purchase_id).first()
    if not db_purchase:
        raise HTTPException(status_code=404, detail="Purchase not found")
    return db_purchase

@router.get("/", response_model=List[schemas.PurchaseOut])
async def list_purchases(
    client_id: Optional[int] = None,
    product_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    query = db.query(models.Purchase)

    if client_id:
        query = query.filter(models.Purchase.client_id == client_id)

    if product_id:
        query = query.filter(models.Purchase.product_id == product_id)

    purchases = query.offset(skip).limit(limit).all()
    return purchases

@router.delete("/{purchase_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_purchase(
    purchase_id: int,
    db: Session = Depends(get_db)
):
    db_purchase = db.query(models.Purchase).filter(models.Purchase.purchase_id == purchase_id).first()
    if not db_purchase:
        raise HTTPException(status_code=404, detail="Purchase not found")

    # Возвращаем товар на склад
    product = db.query(models.Product).filter(models.Product.product_id == db_purchase.product_id).first()
    if product:
        product.quantity += db_purchase.quantity

    db.delete(db_purchase)
    db.commit()

    try:
        # Отправляем уведомление об удалении
        await producers.send_purchase({
            "purchase_id": purchase_id,
            "deleted": True
        })
        logger.info(f"Purchase deletion {purchase_id} sent to Kafka")
    except Exception as e:
        logger.error(f"Failed to send purchase deletion to Kafka: {str(e)}")

    return None
