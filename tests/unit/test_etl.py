import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
from app.db.models import Biometric
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from app.etl.run_etl import (
    normalize_units,
    validate_biometric_ranges,
    load_patient_data,
    validate_patient_row,
    process_patients,
    upsert_patients,
    save_invalid_patients,
    get_simulated_files,
    read_biometric_chunks,
    validate_biometric_chunk,
    process_biometric_records,
    upsert_biometric_records,
    save_invalid_biometrics,
    run_etl
)


# ---- Test Utility Functions ----

def test_normalize_units():
    # Test weight conversion from lbs to kg
    assert normalize_units(150, "lbs", "weight") == pytest.approx(68.0388)
    # Test weight in kg remains same
    assert normalize_units(70, "kg", "weight") == 70
    # Test unknown metric type returns original value
    assert normalize_units(100, "bpm", "heart_rate") == 100


def test_validate_biometric_ranges():
    # Test valid glucose
    assert validate_biometric_ranges({"biometric_type": "glucose", "value": "100"}) == []

    # Test out of range glucose
    assert "out of range" in validate_biometric_ranges({"biometric_type": "glucose", "value": "300"})[0]

    # Test valid blood pressure
    assert validate_biometric_ranges({"biometric_type": "blood_pressure", "value": "120/80"}) == []

    # Test invalid blood pressure format
    assert "Invalid blood pressure format" in \
           validate_biometric_ranges({"biometric_type": "blood_pressure", "value": "120-80"})[0]

    # Test invalid value type
    assert "Invalid value" in validate_biometric_ranges({"biometric_type": "glucose", "value": "high"})[0]


# ---- Test Patient ETL ----

@patch('app.etl.run_etl.pd.read_json')
def test_load_patient_data_success(mock_read_json):
    mock_read_json.return_value = pd.DataFrame([{"name": "John Doe"}])
    result = load_patient_data("dummy.json")
    assert not result.empty
    assert "name" in result.columns


@patch('app.etl.run_etl.pd.read_json')
def test_load_patient_data_failure(mock_read_json, caplog):
    mock_read_json.side_effect = Exception("File error")
    result = load_patient_data("invalid.json")
    assert result.empty
    assert "Failed to load patient JSON" in caplog.text


def test_validate_patient_row_valid():
    valid_row = pd.Series({
        "name": "John Doe",
        "email": "john@example.com",
        "dob": "1980-01-01",
        "gender": "Male",
        "address": "123 Main St",
        "phone": "555-1234",
        "sex": "M"
    })
    is_valid, err = validate_patient_row(valid_row)
    assert is_valid
    assert err == ""


def test_validate_patient_row_invalid_age():
    invalid_row = pd.Series({
        "name": "John Doe",
        "email": "john@example.com",
        "dob": "1800-01-01",  # Too old
        "gender": "Male",
        "address": "123 Main St",
        "phone": "555-1234",
        "sex": "M"
    })
    is_valid, err = validate_patient_row(invalid_row)
    assert not is_valid
    assert "Implausible age" in err


@patch('app.etl.run_etl.upsert_patients')
@patch('app.etl.run_etl.save_invalid_patients')
@patch('app.etl.run_etl.load_patient_data')
def test_process_patients(mock_load, mock_save, mock_upsert):
    # Setup mock return value
    test_data = pd.DataFrame([{
        "name": "John Doe",
        "email": "john@example.com",
        "dob": "1980-01-01",
        "gender": "Male",
        "address": "123 Main St",
        "phone": "555-1234",
        "sex": "M"
    }])
    mock_load.return_value = test_data

    process_patients("dummy.json")

    # Verify mocks were called
    mock_load.assert_called_once_with("dummy.json")
    mock_upsert.assert_called_once()
    mock_save.assert_called_once()


def test_save_invalid_patients(tmp_path):
    invalid_rows = [{"name": "Bad Data", "error": "Invalid format"}]
    with patch('app.etl.run_etl.os.makedirs'), \
            patch('app.etl.run_etl.pd.DataFrame.to_json') as mock_to_json:
        save_invalid_patients(invalid_rows)
        mock_to_json.assert_called_once()


# ---- Test Biometric ETL ----

def test_get_simulated_files(tmp_path):
    test_dir = tmp_path / "biometrics"
    test_dir.mkdir()
    (test_dir / "biometrics_2023-01-01T12-00.csv").touch()

    with patch('app.etl.run_etl.BIOMETRICS_DIR', str(test_dir)):
        files = get_simulated_files()
        assert len(files) == 1
        assert "2023-01-01T12-00.csv" in files[0]


@patch('app.etl.run_etl.pd.read_csv')
def test_read_biometric_chunks(mock_read_csv):
    mock_read_csv.return_value = [pd.DataFrame([{"patient_email": "test@example.com"}])]
    chunks, invalids = read_biometric_chunks("dummy.csv")
    assert len(list(chunks)) == 1
    assert isinstance(invalids, list)


def test_validate_biometric_chunk_valid():
    valid_data = pd.DataFrame({
        "patient_email": ["test@example.com"],
        "biometric_type": ["glucose"],
        "value": ["100"],
        "timestamp": ["2023-01-01"],
        "unit": ["mg/dL"]
    })
    valid_chunk, invalids = validate_biometric_chunk(valid_data)
    assert len(valid_chunk) == 1
    assert not invalids


def test_process_biometric_records():
    test_chunk = pd.DataFrame({
        "patient_email": ["test@example.com"],
        "biometric_type": ["glucose"],
        "value": ["100"],
        "timestamp": ["2023-01-01"],
        "unit": ["mg/dL"]
    })
    patients_map = {"test@example.com": 1}
    records, invalids = process_biometric_records(test_chunk, patients_map)
    assert len(records) == 1
    assert records[0]["patient_id"] == 1
    assert not invalids


# ---- Integration Style Tests ----

@patch('app.etl.run_etl.process_patients')
@patch('app.etl.run_etl.process_biometrics')
def test_run_etl(mock_biometrics, mock_patients):
    run_etl()
    assert mock_patients.called
    assert mock_biometrics.called


# ---- Test Error Handling ----

def test_upsert_patients_integrity_error(db_session):
    test_records = [{"email": "test@example.com", "name": "Test User"}]

    # Simulate IntegrityError
    db_session.execute = MagicMock(side_effect=IntegrityError("", "", ""))

    with pytest.raises(Exception):
        upsert_patients(db_session, test_records)


def test_upsert_biometric_records_duplicate(db_session):
    records = [{
        "patient_id": 1,
        "biometric_type": "glucose",
        "value": 100,
        "timestamp": "2023-01-01"
    }]

    # First call raises IntegrityError, subsequent calls work
    db_session.bulk_insert_mappings = MagicMock(side_effect=IntegrityError("", "", ""))
    db_session.execute = MagicMock(return_value=None)

    upsert_biometric_records(db_session, records)
    assert db_session.execute.called