import hashlib
import json

from sqlalchemy.orm import Session

from index_db.models import Product, ProductHistory, Brand, Seller

class BrandRepository:
    @staticmethod
    def get_by_id(db : Session, brand_id : int):
        return db.query(Brand).filter(Brand.id == brand_id).first()

    @staticmethod
    def get_by_name(db: Session, name: str):
        return db.query(Brand).filter(Brand.name == name).first()

    @staticmethod
    def change_url(db : Session, brand_id : int, new_url : str):
        brand = BrandRepository.get_by_id(db, brand_id)
        if brand is None:
            raise KeyError(f"Brand with id {brand_id} does not exist!")
        if brand.url != new_url:
            brand.url = new_url
            db.commit()
            db.refresh(brand)
        return brand

    @staticmethod
    def get_or_create(db: Session, name: str, url: str = None):
        brand = BrandRepository.get_by_name(db, name)
        if not brand:
            brand = Brand(name=name, url=url)
            db.add(brand)
            db.commit()
            db.refresh(brand)
        elif url is not None:
            brand = BrandRepository.change_url(db, brand.id, url)
        return brand

class SellerRepository:
    @staticmethod
    def get_by_id(db : Session, seller_id : int):
        return db.query(Seller).filter(Seller.id == seller_id).first()

    @staticmethod
    def get_by_name(db: Session, name: str):
        return db.query(Seller).filter(Seller.name == name).first()

    @staticmethod
    def change_url(db : Session, seller_id : int, new_url : str):
        seller = SellerRepository.get_by_id(db, seller_id)
        if seller is None:
            raise KeyError(f"Seller with id {seller_id} does not exist!")
        if seller.url != new_url:
            seller.url = new_url
            db.commit()
            db.refresh(seller)
        return seller

    @staticmethod
    def get_or_create(db: Session, name: str, url: str = None):
        seller = SellerRepository.get_by_name(db, name)
        if not seller:
            seller = Seller(name=name, url=url)
            db.add(seller)
            db.commit()
            db.refresh(seller)
        elif url is not None:
            seller = SellerRepository.change_url(db, seller.id, url)
        return seller

class ProductRepository:
    @staticmethod
    def compute_product_hash(product_data: dict) -> str:
        fields = ['name', 'url', 'on_sale', 'price_ozon_card', 'rating', 'review_count', 'brand']
        relevant_data = {k: product_data.get(k) for k in fields}
        data_str = json.dumps(relevant_data, sort_keys=True)
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()

    @staticmethod
    def get_by_id(db : Session, product_id : int):
        return db.query(Product).filter(Product.id == product_id).first()

    @staticmethod
    def get_by_pk(db : Session, product_pk : int):
        return db.query(Product).filter(Product.pk == product_pk).first()

    @staticmethod
    def get_or_create(db : Session, product_description : dict):
        product = ProductRepository.get_by_pk(db, product_description['pk'])
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
    def get_by_seller_id(db : Session, seller_id : int):
        seller_products = db.query(Product).filter(Product.seller_id == seller_id)
        if seller_products is None:
            return None
        current_product = [
            (product, ProductRepository.get_last_state(db, product.id)) for product in seller_products
        ]
        return current_product

    @staticmethod
    def get_by_brand_id(db : Session, brand_id : int):
        brand_products = db.query(Product).filter(Product.brand_id == brand_id)
        if brand_products is None:
            return None
        current_product = [
            (product, ProductRepository.get_last_state(db, product.id)) for product in brand_products
        ]
        return current_product

    @staticmethod
    def get_product_history(db : Session, product_id : int):
        return db.query(ProductHistory).filter(ProductHistory.product_id == product_id).order_by(ProductHistory.created_at.desc())

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
