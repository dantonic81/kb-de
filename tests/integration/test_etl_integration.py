import os
import pytest
import pandas as pd
from sqlalchemy import select
from app.db.models import Patient
from app.etl.run_etl import (
    load_patient_data,
    get_simulated_files,
    read_biometric_chunks,
    validate_biometric_chunk,
    FILE_PREFIX,
    FILE_EXT,
    upsert_patients,
)


# ---- Test Data Setup ----


@pytest.fixture
def test_patients_file(tmp_path):
    """Create a temporary patients.json file for testing"""
    data = [
        {
            "name": "Test User",
            "email": "test@example.com",
            "dob": "1980-01-01",
            "gender": "Male",
            "address": "123 Test St",
            "phone": "555-1234",
            "sex": "M",
        },
        {
            "name": "Invalid User",
            "email": "invalid@example.com",
            "dob": "3000-01-01",  # Invalid future date
            "gender": "Unknown",
            "address": "456 Invalid St",
            "phone": "555-5678",
            "sex": "U",
        },
    ]

    file_path = tmp_path / "patients.json"
    pd.DataFrame(data).to_json(file_path, orient="records")
    return str(file_path)


@pytest.fixture
def test_biometrics_file(tmp_path):
    """Create a temporary biometrics CSV file for testing"""
    data = {
        "patient_email": [
            "test@example.com",
            "test@example.com",
            "nonexistent@example.com",
        ],
        "biometric_type": ["glucose", "blood_pressure", "weight"],
        "value": ["100", "120/80", "150"],
        "timestamp": [
            "2023-01-01T00:00:00",
            "2023-01-01T01:00:00",
            "2023-01-01T02:00:00",
        ],
        "unit": ["mg/dL", "mmHg", "lbs"],
    }

    # Create the biometrics directory
    biometrics_dir = tmp_path / "biometrics_simulated"
    biometrics_dir.mkdir()

    # Create a properly named file with timestamp
    file_name = f"{FILE_PREFIX}2023-01-01T00-00{FILE_EXT}"
    file_path = biometrics_dir / file_name
    pd.DataFrame(data).to_csv(file_path, index=False)
    return str(file_path)


# ---- Patient ETL Tests ----


def test_load_patient_data(test_patients_file):
    """Test loading patient data from JSON file"""
    df = load_patient_data(test_patients_file)
    assert not df.empty
    assert len(df) == 2
    assert "test@example.com" in df["email"].values


# ---- Biometric ETL Tests ----


def test_get_simulated_files(test_biometrics_file, monkeypatch):
    """Test finding biometric files"""
    # Override the BIOMETRICS_DIR to only include our test file
    test_dir = os.path.dirname(test_biometrics_file)
    monkeypatch.setattr("app.etl.run_etl.BIOMETRICS_DIR", test_dir)

    files = get_simulated_files()
    assert len(files) == 1
    assert os.path.basename(files[0]).startswith(FILE_PREFIX)
    assert files[0].endswith(FILE_EXT)


def test_read_biometric_chunks(test_biometrics_file):
    """Test reading biometric data in chunks"""
    chunks, invalids = read_biometric_chunks(test_biometrics_file)
    chunk = next(chunks)
    assert len(chunk) == 3  # All rows should be read
    assert len(invalids) == 0  # No bad lines in our test data


def test_validate_biometric_chunk(test_biometrics_file):
    """Test validation of biometric data chunks"""
    chunks, _ = read_biometric_chunks(test_biometrics_file)
    chunk = next(chunks)

    valid_chunk, invalid_rows = validate_biometric_chunk(chunk)
    assert len(valid_chunk) == 3  # All rows pass schema validation
    assert len(invalid_rows) == 0

    # Test with invalid data
    invalid_chunk = chunk.copy()
    invalid_chunk.loc[0, "biometric_type"] = "invalid_type"
    valid_chunk, invalid_rows = validate_biometric_chunk(invalid_chunk)
    assert len(valid_chunk) == 2
    assert len(invalid_rows) == 1


def test_upsert_patients(db_session):
    """Test patient upsert functionality"""
    test_records = [
        {
            "name": "Test User",
            "email": "test@example.com",
            "dob": "1980-01-01",
            "gender": "Male",
            "address": "123 Test St",
            "phone": "555-1234",
            "sex": "M",
        }
    ]

    # First insert
    upsert_patients(db_session, test_records)
    patient = db_session.execute(
        select(Patient).where(Patient.email == "test@example.com")
    ).scalar_one()
    assert patient.name == "Test User"

    # Test update
    test_records[0]["name"] = "Updated Name"
    upsert_patients(db_session, test_records)
    db_session.refresh(patient)
    assert patient.name == "Updated Name"
