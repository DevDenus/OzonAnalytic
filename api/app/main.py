from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session

from index_db.db import get_db
from index_db.models import Product
from index_db.operations import ProductRepository

app = FastAPI()


@app.get("/products")
def read_products(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    products = db.query(Product).offset(skip).limit(limit).all()
    return products


@app.get("/products/{product_id}")
def get_product(product_id: int, db: Session = Depends(get_db)):
    product = ProductRepository.get(db, product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="Product not found")
    return product
