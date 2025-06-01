import pytest
from datetime import datetime, timedelta
from app.db import models

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
