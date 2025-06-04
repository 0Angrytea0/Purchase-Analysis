from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from ..db import get_db
from .. import schemas, models

router = APIRouter(prefix="/sellers", tags=["sellers"])

@router.post("/", response_model=schemas.SellerOut)
async def create_seller(
    seller: schemas.SellerCreate,
    db: Session = Depends(get_db)
):
    db_seller = models.Seller(
        first_name=seller.first_name,
        last_name=seller.last_name,
        company_name=seller.company_name,
        email=seller.email
    )
    db.add(db_seller)
    db.commit()
    db.refresh(db_seller)
    from ..producers import send_seller
    await send_seller(seller.dict())

    return db_seller

@router.get("/{seller_id}", response_model=schemas.SellerOut)
async def get_seller(
    seller_id: int,
    db: Session = Depends(get_db)
):
    seller = db.query(models.Seller).filter(models.Seller.seller_id == seller_id).first()
    if not seller:
        raise HTTPException(status_code=404, detail="Seller not found")
    return seller

@router.get("/", response_model=List[schemas.SellerOut])
async def list_sellers(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    sellers = db.query(models.Seller).offset(skip).limit(limit).all()
    return sellers

@router.put("/{seller_id}", response_model=schemas.SellerOut)
async def update_seller(
    seller_id: int,
    seller_data: schemas.SellerUpdate,
    db: Session = Depends(get_db)
):
    db_seller = db.query(models.Seller).filter(models.Seller.seller_id == seller_id).first()
    if not db_seller:
        raise HTTPException(status_code=404, detail="Seller not found")

    update_data = seller_data.dict(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_seller, key, value)

    db.commit()
    db.refresh(db_seller)
    return db_seller

@router.delete("/{seller_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_seller(
    seller_id: int,
    db: Session = Depends(get_db)
):
    db_seller = db.query(models.Seller).filter(models.Seller.seller_id == seller_id).first()
    if not db_seller:
        raise HTTPException(status_code=404, detail="Seller not found")

    db.delete(db_seller)
    db.commit()
    return None
