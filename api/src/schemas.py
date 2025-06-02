from pydantic import BaseModel, Field, EmailStr
from typing import Optional
from datetime import datetime

class ClientBase(BaseModel):
    first_name: str = Field(..., max_length=100)
    last_name: str = Field(..., max_length=100)
    email: EmailStr = Field(..., max_length=255)

class ClientCreate(ClientBase):
    pass

class ClientUpdate(BaseModel):
    first_name: Optional[str] = Field(None, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    email: Optional[EmailStr] = Field(None, max_length=255)

class ClientOut(ClientBase):
    client_id: int
    signup_date: datetime

    class Config:
        orm_mode = True

class SellerBase(BaseModel):
    first_name: str = Field(..., max_length=100)
    last_name: str = Field(..., max_length=100)
    company_name: str = Field(..., max_length=200)
    email: EmailStr = Field(..., max_length=255)

class SellerCreate(SellerBase):
    pass

class SellerUpdate(BaseModel):
    first_name: Optional[str] = Field(None, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    company_name: Optional[str] = Field(None, max_length=200)
    email: Optional[EmailStr] = Field(None, max_length=255)

class SellerOut(SellerBase):
    seller_id: int
    created_at: datetime

    class Config:
        orm_mode = True

class ProductBase(BaseModel):
    seller_id: int
    name: str = Field(..., max_length=200)
    price: float = Field(..., gt=0)
    quantity: int = Field(1, ge=1)

class ProductCreate(ProductBase):
    pass

class ProductUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=200)
    price: Optional[float] = Field(None, gt=0)
    quantity: Optional[int] = Field(None, ge=1)

class ProductOut(ProductBase):
    product_id: int
    created_at: datetime

    class Config:
        orm_mode = True

class PurchaseBase(BaseModel):
    client_id: int
    product_id: int
    quantity: int = Field(1, ge=1)

class PurchaseCreate(PurchaseBase):
    pass

class PurchaseOut(PurchaseBase):
    purchase_id: int
    purchased_at: datetime

    class Config:
        orm_mode = True

class RawLoadLogOut(BaseModel):
    filename: str
    loaded_at: datetime

    class Config:
        orm_mode = True
