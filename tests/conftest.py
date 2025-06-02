from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db.base import Base
import pytest
from app.main import app
from app.db.session import get_db
from fastapi.testclient import TestClient
import pandas as pd


# Add check_same_thread=False to allow multi-threaded access
TEST_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@pytest.fixture(scope="session")
def db_engine():
    Base.metadata.create_all(bind=engine)
    return engine

@pytest.fixture(scope="function")
def db_session(db_engine):
    connection = db_engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)

    yield session

    session.close()
    transaction.rollback()
    connection.close()

@pytest.fixture
def client(db_session):
    def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()

@pytest.fixture(autouse=True)
def override_get_db(client, db_session):
    def _get_db():
        try:
            yield db_session
        finally:
            pass
    client.app.dependency_overrides[get_db] = _get_db
    yield
    client.app.dependency_overrides.clear()


@pytest.fixture
def sample_patient_data():
    return pd.DataFrame([{
        "name": "Test User",
        "email": "test@example.com",
        "dob": "1980-01-01",
        "gender": "Male",
        "address": "123 Test St",
        "phone": "555-1234",
        "sex": "M"
    }])

@pytest.fixture
def sample_biometric_data():
    return pd.DataFrame({
        "patient_email": ["test@example.com"],
        "biometric_type": ["glucose"],
        "value": ["100"],
        "timestamp": ["2023-01-01T00:00:00"],
        "unit": ["mg/dL"]
    })
