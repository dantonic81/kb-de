from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Format: postgresql+psycopg2://<username>:<password>@<host>:<port>/<database>
SQLALCHEMY_DATABASE_URL = "postgresql+psycopg2://user:password@localhost:5432/health_data"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()