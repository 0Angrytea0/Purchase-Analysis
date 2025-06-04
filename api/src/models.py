from sqlalchemy import Column, Integer, String, Numeric, DateTime, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from .db import Base

class Client(Base):
    __tablename__ = "clients"

    client_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    email = Column(String(255), unique=True)
    signup_date = Column(DateTime(timezone=True), server_default=func.now())
    purchases = relationship("Purchase", back_populates="client")

class Seller(Base):
    __tablename__ = "sellers"

    seller_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    company_name = Column(String(200), nullable=False)
    email = Column(String(255), unique=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    products = relationship("Product", back_populates="seller")

class Product(Base):
    __tablename__ = "products"

    product_id = Column(Integer, primary_key=True, autoincrement=True)
    seller_id = Column(Integer, ForeignKey('sellers.seller_id', ondelete='CASCADE'), nullable=False)
    name = Column(String(200), nullable=False)
    price = Column(Numeric(12, 2), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    seller = relationship("Seller", back_populates="products")
    purchases = relationship("Purchase", back_populates="product")

class Purchase(Base):
    __tablename__ = "purchases"

    purchase_id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(Integer, ForeignKey('clients.client_id'), nullable=False)
    product_id = Column(Integer, ForeignKey('products.product_id'), nullable=False)
    quantity = Column(Integer, nullable=False, default=1)
    purchased_at = Column(DateTime(timezone=True), server_default=func.now())
    client = relationship("Client", back_populates="purchases")
    product = relationship("Product", back_populates="purchases")

class RawLoadLog(Base):
    __tablename__ = "raw_load_log"

    filename = Column(String, primary_key=True)
    loaded_at = Column(DateTime(timezone=True), server_default=func.now())
