import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.db import models
from app.db.session import get_db


@pytest.fixture
def client(db_session):
    # Override FastAPI's dependency to use the test DB session
    def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def test_list_patients_empty(client):
    response = client.get("/patients/")
    assert response.status_code == 200
    assert response.json() == []


def test_list_patients_with_data(client, db_session):
    patients = [
        models.Patient(
            name="Alice",
            dob="1994-06-01",
            gender="female",
            address="123 A St",
            email="alice@example.com",
            phone="1234567890",
            sex="F"
        ),
        models.Patient(
            name="Bob",
            dob="1984-06-01",
            gender="male",
            address="456 B St",
            email="bob@example.com",
            phone="2345678901",
            sex="M"
        ),
        models.Patient(
            name="Carol",
            dob="1974-06-01",
            gender="female",
            address="789 C St",
            email="carol@example.com",
            phone="3456789012",
            sex="F"
        ),
    ]
    db_session.add_all(patients)
    db_session.commit()

    response = client.get("/patients/")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 3
    assert data[0]["name"] == "Alice"
    assert data[1]["name"] == "Bob"


def test_list_patients_pagination(client, db_session):
    for i in range(20):
        db_session.add(models.Patient(
            name=f"Patient {i}",
            dob="1990-01-01",
            gender="other",
            address=f"{i} Test Lane",
            email=f"patient{i}@example.com",
            phone=f"555000{i:03d}",
            sex="O"
        ))
    db_session.commit()

    response = client.get("/patients/?skip=10&limit=5")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 5
    assert data[0]["name"] == "Patient 10"
