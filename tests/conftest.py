from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.db.base import Base
import pytest
from app.main import app
from app.db.session import get_db
from fastapi.testclient import TestClient
import pandas as pd
from app.etl.run_etl import FILE_PREFIX, FILE_EXT


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


@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch):
    """Override the database URL to use SQLite for testing"""
    monkeypatch.setattr('app.etl.run_etl.DATABASE_URL', 'sqlite:///:memory:')
    monkeypatch.setattr('app.etl.run_etl._engine', None)  # Reset engine
    monkeypatch.setattr('app.etl.run_etl._Session', None)  # Reset sessionmaker

@pytest.fixture(autouse=True)
def setup_db_tables(db_engine):
    """Ensure database tables are created before each test"""
    from app.db.models import Base
    Base.metadata.create_all(bind=db_engine)
    yield
    Base.metadata.drop_all(bind=db_engine)


@pytest.fixture
def test_patients_file(tmp_path):
    """Create a temporary patients.json file for testing"""
    data = [{
        "name": "Test User",
        "email": "test@example.com",
        "dob": "1980-01-01",
        "gender": "Male",
        "address": "123 Test St",
        "phone": "555-1234",
        "sex": "M"
    }, {
        "name": "Invalid User",
        "email": "invalid@example.com",
        "dob": "1800-01-01",  # Changed from 3000 to 1800 to avoid timestamp issue
        "gender": "Unknown",
        "address": "456 Invalid St",
        "phone": "555-5678",
        "sex": "U"
    }]

    file_path = tmp_path / "patients.json"
    pd.DataFrame(data).to_json(file_path, orient="records")
    return str(file_path)
