from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
from ..db import get_db
from .. import schemas, models, producers
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/products", tags=["products"])

@router.post("/", response_model=schemas.ProductOut)
async def create_product(
    product: schemas.ProductCreate,
    db: Session = Depends(get_db)
):
    # Проверяем существование продавца
    seller = db.query(models.Seller).filter(models.Seller.seller_id == product.seller_id).first()
    if not seller:
        raise HTTPException(status_code=404, detail="Seller not found")

    if product.price <= 0:
        raise HTTPException(status_code=400, detail="Price must be positive")

    db_product = models.Product(
        seller_id=product.seller_id,
        name=product.name,
        price=product.price,
        quantity=product.quantity if hasattr(product, 'quantity') else 1
    )

    db.add(db_product)
    db.commit()
    db.refresh(db_product)

    try:
        product_data = {
            "product_id": db_product.product_id,
            "seller_id": db_product.seller_id,
            "name": db_product.name,
            "price": float(db_product.price),
            "quantity": db_product.quantity,
            "created_at": db_product.created_at.isoformat()
        }
        await producers.send_product(product_data)
        logger.info(f"Product {db_product.product_id} sent to Kafka")
    except Exception as e:
        logger.error(f"Failed to send product to Kafka: {str(e)}")

    return db_product

@router.get("/{product_id}", response_model=schemas.ProductOut)
async def get_product(
    product_id: int,
    db: Session = Depends(get_db)
):
    db_product = db.query(models.Product).filter(models.Product.product_id == product_id).first()
    if not db_product:
        raise HTTPException(status_code=404, detail="Product not found")
    return db_product

@router.get("/", response_model=List[schemas.ProductOut])
async def list_products(
    seller_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    query = db.query(models.Product)

    if seller_id:
        query = query.filter(models.Product.seller_id == seller_id)

    products = query.offset(skip).limit(limit).all()
    return products

@router.put("/{product_id}", response_model=schemas.ProductOut)
async def update_product(
    product_id: int,
    product_data: schemas.ProductUpdate,
    db: Session = Depends(get_db)
):
    db_product = db.query(models.Product).filter(models.Product.product_id == product_id).first()
    if not db_product:
        raise HTTPException(status_code=404, detail="Product not found")

    update_data = product_data.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_product, key, value)

    db.commit()
    db.refresh(db_product)

    try:
        updated_data = {
            "product_id": product_id,
            "name": db_product.name,
            "price": float(db_product.price),
            "quantity": db_product.quantity
        }
        await producers.send_product(updated_data)
        logger.info(f"Updated product {product_id} sent to Kafka")
    except Exception as e:
        logger.error(f"Failed to send updated product to Kafka: {str(e)}")

    return db_product

@router.delete("/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_product(
    product_id: int,
    db: Session = Depends(get_db)
):
    db_product = db.query(models.Product).filter(models.Product.product_id == product_id).first()
    if not db_product:
        raise HTTPException(status_code=404, detail="Product not found")

    db.delete(db_product)
    db.commit()

    try:
        await producers.send_product({
            "product_id": product_id,
            "deleted": True
        })
        logger.info(f"Product deletion {product_id} sent to Kafka")
    except Exception as e:
        logger.error(f"Failed to send product deletion to Kafka: {str(e)}")

    return None
