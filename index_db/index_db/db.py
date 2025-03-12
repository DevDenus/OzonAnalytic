from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

from index_db.models import Base

def init_db(db_url : str):
    engine = create_engine(db_url, echo=False)
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    if not tables:
        Base.metadata.create_all(bind=engine)
        print(f"Database was created with tables {list(Base.metadata.tables.keys())}")
    else:
        print(f"Database already exist with tables: {tables}")

def get_db(db_url : str):
    engine = create_engine(db_url, echo=False)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
