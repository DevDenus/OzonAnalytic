from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import datetime

Base = declarative_base()

class Brand(Base):
    __tablename__ = "brands"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, unique=True)
    url = Column(String, nullable=True)
    last_update = Column(DateTime, default=datetime.datetime.now(datetime.timezone.utc))
    products = relationship("Product", back_populates="brand")

class Seller(Base):
    __tablename__ = "sellers"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, unique=True)
    url = Column(String, nullable=True)
    last_update = Column(DateTime, default=datetime.datetime.now(datetime.timezone.utc))
    products = relationship("Product", back_populates="seller")

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True, index=True)
    pk = Column(Integer, unique=True, index=True)
    name = Column(String, nullable=False)
    url = Column(String, unique=True)

    brand_id = Column(Integer, ForeignKey("brands.id"), nullable=True)
    seller_id = Column(Integer, ForeignKey("sellers.id"), nullable=False)

    brand = relationship("Brand", back_populates="products")
    seller = relationship("Seller", back_populates="products")

    product_history = relationship("ProductHistory", back_populates="product")

class ProductHistory(Base):
    __tablename__ = "products_history"

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    price = Column(Float, nullable=False)
    price_ozon_card = Column(Float, nullable=False)
    rating = Column(Float, default=0)
    review_count = Column(Integer, default=0)
    question_count = Column(Integer, default=0)
    on_sale = Column(Boolean, default=False)
    hash = Column(String, nullable=False)

    created_at = Column(DateTime, default=datetime.datetime.now(datetime.timezone.utc))

    product = relationship("Product", back_populates="product_history")
