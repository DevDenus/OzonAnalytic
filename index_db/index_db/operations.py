import hashlib
import json

from sqlalchemy.orm import Session

from index_db.models import Product, ProductHistory, Brand, Seller

class BrandRepository:
    @staticmethod
    def get(db: Session, name: str):
        brand = db.query(Brand).filter(Brand.name == name).first()
        return brand

    @staticmethod
    def get_or_create(db: Session, name: str, url: str = None):
        brand = db.query(Brand).filter(Brand.name == name).first()
        if not brand:
            brand = Brand(name=name, url=url)
            db.add(brand)
            db.commit()
            db.refresh(brand)
        return brand

class SellerRepository:
    @staticmethod
    def get(db: Session, name: str):
        seller = db.query(Seller).filter(Seller.name == name).first()
        return seller

    @staticmethod
    def get_or_create(db: Session, name: str, url: str = None):
        seller = db.query(Seller).filter(Seller.name == name).first()
        if not seller:
            seller = Seller(name=name, url=url)
            db.add(seller)
            db.commit()
            db.refresh(seller)
        return seller

class ProductRepository:
    @staticmethod
    def compute_product_hash(product_data: dict) -> str:
        fields = ['name', 'url', 'on_sale', 'price_ozon_card', 'rating', 'review_count', 'brand']
        relevant_data = {k: product_data.get(k) for k in fields}
        data_str = json.dumps(relevant_data, sort_keys=True)
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()

    @staticmethod
    def get(db : Session, product_pk : int):
        product = db.query(Product).filter(Product.pk == product_pk).first()
        return product

    @staticmethod
    def get_or_create(db : Session, product_description : dict):
        product = db.query(Product).filter(Product.pk == product_description['pk']).first()
        if not product:
            product = Product(
                pk=product_description['pk'], name=product_description['name'], url=product_description['url'], brand_id=product_description['brand_id'],
                seller_id=product_description['seller_id']
            )
            db.add(product)
            db.commit()
            db.refresh(product)
        return product

    @staticmethod
    def get_last_state(db: Session, product_id: int):
        return db.query(ProductHistory).filter(ProductHistory.product_id == product_id).order_by(ProductHistory.created_at.desc()).first()

    @staticmethod
    def add_state(db: Session, product_id: int, product_description: dict):
        new_hash = ProductRepository.compute_product_hash(product_description)
        last_price_entry = ProductRepository.get_last_state(db, product_id)

        if last_price_entry and last_price_entry.hash == new_hash:
            return None

        new_price_entry = ProductHistory(
            product_id=product_id,
            price=product_description['price'],
            price_ozon_card=product_description['price_ozon_card'],
            rating=product_description['rating'],
            review_count=product_description['review_count'],
            question_count=product_description['question_count'],
            on_sale=product_description['on_sale'],
            hash=new_hash
        )
        db.add(new_price_entry)
        db.commit()
        db.refresh(new_price_entry)
        return new_price_entry
