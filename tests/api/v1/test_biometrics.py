import pytest
from datetime import datetime, timedelta
from app.db import models
from app.db.models import Biometric, Patient
from fastapi import status
from fastapi.testclient import TestClient



# tests for 2nd endpoint
def test_list_biometrics_patient_not_found(client):
    response = client.get("/biometrics/9999")  # non-existent patient
    assert response.status_code == 404
    assert response.json()["detail"] == "Patient 9999 not found"

def test_list_biometrics_empty(client, db_session):
    patient = models.Patient(name="Test Patient", dob="1990-01-01")
    db_session.add(patient)
    db_session.commit()

    response = client.get(f"/biometrics/{patient.id}")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 0
    assert data["data"] == []
    assert data["skip"] == 0
    assert data["limit"] == 10

def test_list_biometrics_with_data(client, db_session):
    patient = models.Patient(name="Test Patient", dob="1990-01-01")
    db_session.add(patient)
    db_session.commit()

    # Add some weight biometrics
    biometrics_weight = [
        models.Biometric(
            patient_id=patient.id,
            biometric_type="weight",
            value=70.0 + i,
            unit="kg",
            timestamp=datetime.utcnow() - timedelta(days=i)
        )
        for i in range(5)
    ]
    # Add some blood pressure biometrics
    biometrics_bp = [
        models.Biometric(
            patient_id=patient.id,
            biometric_type="blood_pressure",
            systolic=120 + i,
            diastolic=80 + i,
            unit="mmHg",
            timestamp=datetime.utcnow() - timedelta(days=10 + i)
        )
        for i in range(3)
    ]

    db_session.add_all(biometrics_weight + biometrics_bp)
    db_session.commit()

    # Get all biometrics (default skip=0, limit=10)
    response = client.get(f"/biometrics/{patient.id}")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 8
    assert len(data["data"]) == 8
    assert data["skip"] == 0
    assert data["limit"] == 10

    # Check ordering by timestamp desc
    timestamps = [entry["timestamp"] for entry in data["data"]]
    assert timestamps == sorted(timestamps, reverse=True)

    # Filter by type=weight (case insensitive)
    response = client.get(f"/biometrics/{patient.id}?type=weight")
    data = response.json()
    assert data["total"] == 5
    assert all(b["biometric_type"] == "weight" for b in data["data"])

    # Pagination: skip 2, limit 3
    response = client.get(f"/biometrics/{patient.id}?skip=2&limit=3")
    data = response.json()
    assert data["skip"] == 2
    assert data["limit"] == 3
    assert len(data["data"]) == 3


# tests for 3rd endpoint
def test_upsert_biometric_create(client, db_session, db_engine):
    if "sqlite" in str(db_engine.dialect.name):
        pytest.skip("SQLite does not support native ON CONFLICT upsert")
    # Create patient first
    patient = models.Patient(name="Test Patient", dob="1990-01-01")
    db_session.add(patient)
    db_session.commit()

    payload = {
        "biometric_type": "weight",
        "value": 70.5,
        "unit": "kg",
        "timestamp": datetime.utcnow().isoformat(),
        "systolic": None,
        "diastolic": None
    }

    response = client.post(f"/biometrics/{patient.id}", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["patient_id"] == patient.id
    assert data["biometric_type"] == "weight"
    assert data["value"] == 70.5
    assert data["unit"] == "kg"


def test_upsert_biometric_update(client, db_engine, db_session):
    if "sqlite" in str(db_engine.dialect.name):
        pytest.skip("SQLite does not support native ON CONFLICT upsert")
    # Create patient and biometric first
    patient = models.Patient(name="Test Patient 2", dob="1990-01-01")
    db_session.add(patient)
    db_session.commit()

    timestamp = datetime.utcnow()

    biometric = models.Biometric(
        patient_id=patient.id,
        biometric_type="weight",
        value=70.5,
        unit="kg",
        timestamp=timestamp
    )
    db_session.add(biometric)
    db_session.commit()

    # Update biometric value
    payload = {
        "biometric_type": "weight",
        "value": 72.0,
        "unit": "kg",
        "timestamp": timestamp.isoformat(),
        "systolic": None,
        "diastolic": None
    }

    response = client.post(f"/biometrics/{patient.id}", json=payload)
    assert response.status_code == 201
    data = response.json()
    assert data["value"] == 72.0  # Updated value


def test_upsert_biometric_patient_not_found(client):
    payload = {
        "biometric_type": "weight",
        "value": 70.5,
        "unit": "kg",
        "timestamp": datetime.utcnow().isoformat(),
        "systolic": None,
        "diastolic": None
    }

    response = client.post("/biometrics/9999", json=payload)  # Nonexistent patient ID
    assert response.status_code == 404
    assert response.json()["detail"] == "Patient 9999 not found"


@pytest.mark.parametrize(
    "payload, error_detail",
    [
        (
            {
                "biometric_type": "blood_pressure",
                "value": None,
                "unit": "mmHg",
                "timestamp": datetime.utcnow().isoformat(),
                "systolic": None,
                "diastolic": 80
            },
            "Both systolic and diastolic are required for blood pressure"
        ),
        (
            {
                "biometric_type": "blood_pressure",
                "value": None,
                "unit": "mmHg",
                "timestamp": datetime.utcnow().isoformat(),
                "systolic": 120,
                "diastolic": None
            },
            "Both systolic and diastolic are required for blood pressure"
        ),
    ]
)
def test_upsert_biometric_validation_error(client, db_session, payload, error_detail):
    patient = models.Patient(name="Test Patient 3", dob="1990-01-01")
    db_session.add(patient)
    db_session.commit()

    response = client.post(f"/biometrics/{patient.id}", json=payload)
    assert response.status_code == 422
    assert response.json()["detail"] == error_detail

# tests for 4th endpoint
@pytest.mark.usefixtures("db_session")
def test_delete_biometric_success(client: TestClient, db_session):
    # Create a patient and biometric record
    patient = Patient(name="Test Patient")
    db_session.add(patient)
    db_session.commit()

    biometric = Biometric(
        patient_id=patient.id,
        biometric_type="weight",
        value=70.5,
        unit="kg",
        timestamp=datetime(2024, 1, 1, 10, 0, 0)
    )
    db_session.add(biometric)
    db_session.commit()

    # Perform DELETE request
    response = client.delete(f"/biometrics/{biometric.id}")

    assert response.status_code == status.HTTP_204_NO_CONTENT

    # Verify record was deleted
    deleted = db_session.query(Biometric).get(biometric.id)
    assert deleted is None

@pytest.mark.usefixtures("db_session")
def test_delete_biometric_not_found(client: TestClient):
    # Delete non-existing ID
    response = client.delete("/biometrics/99999")
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert "not found" in response.json()["detail"].lower()

@pytest.mark.usefixtures("db_session")
def test_delete_biometric_db_error(client: TestClient, monkeypatch, db_session):
    # Create patient and biometric to delete
    patient = Patient(name="Test Patient")
    db_session.add(patient)
    db_session.commit()

    biometric = Biometric(
        patient_id=patient.id,
        biometric_type="weight",
        value=70.5,
        unit="kg",
        timestamp=datetime(2024, 1, 1, 10, 0, 0)
    )
    db_session.add(biometric)
    db_session.commit()

    # Monkeypatch db_session.commit to raise SQLAlchemyError
    from sqlalchemy.exc import SQLAlchemyError
    def raise_error():
        raise SQLAlchemyError("DB failure")

    monkeypatch.setattr(db_session, "commit", raise_error)

    response = client.delete(f"/biometrics/{biometric.id}")

    assert response.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "database error" in response.json()["detail"].lower()
