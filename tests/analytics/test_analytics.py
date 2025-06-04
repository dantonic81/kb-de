import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from app.analytics.analytics import (
    analytics_aggregate_biometrics,
    get_basic_biometrics_query,
    get_blood_pressure_query,
    aggregate_hourly,
    load_biometrics_data,
    upsert_aggregates,
)
from datetime import datetime


@pytest.fixture
def sample_raw_data():
    return pd.DataFrame(
        {
            "patient_id": [1, 1, 1],
            "biometric_type": ["glucose", "glucose", "weight"],
            "hour_start": [datetime(2023, 1, 1, 0)] * 3,
            "value": [100.0, 120.0, 70.5],
        }
    )


def test_get_basic_biometrics_query():
    sql = get_basic_biometrics_query()
    assert "SELECT" in sql
    assert "biometric_type IN ('glucose', 'weight')" in sql


def test_get_blood_pressure_query():
    sql = get_blood_pressure_query()
    assert "systolic" in sql
    assert "diastolic" in sql
    assert "UNION ALL" in sql


def test_aggregate_hourly(sample_raw_data):
    agg = aggregate_hourly(sample_raw_data)

    assert agg.shape[0] == 2  # 2 unique combinations: glucose, weight
    assert set(agg.columns) == {
        "patient_id",
        "biometric_type",
        "hour_start",
        "min_value",
        "max_value",
        "avg_value",
        "count",
    }

    glucose = agg[agg["biometric_type"] == "glucose"].iloc[0]
    assert glucose["min_value"] == 100.0
    assert glucose["max_value"] == 120.0
    assert glucose["avg_value"] == 110.0  # Rounded
    assert glucose["count"] == 2

    weight = agg[agg["biometric_type"] == "weight"].iloc[0]
    assert weight["avg_value"] == 70.5


@patch("app.analytics.analytics.engine")
@patch("pandas.read_sql")
def test_load_biometrics_data(mock_read_sql, mock_engine, sample_raw_data):
    mock_read_sql.side_effect = [sample_raw_data.iloc[:2], sample_raw_data.iloc[2:]]

    df = load_biometrics_data()

    assert df.shape[0] == 3
    assert set(df.columns) == {"patient_id", "biometric_type", "hour_start", "value"}


@patch("app.analytics.analytics.Session")
@patch("app.db.models.PatientBiometricHourlySummary")
def test_upsert_aggregates(mock_model, mock_session_class, sample_raw_data):
    mock_session = MagicMock()
    mock_session_class.return_value.__enter__.return_value = mock_session

    agg_df = aggregate_hourly(sample_raw_data)
    upserted_count = upsert_aggregates(agg_df)

    assert upserted_count == len(agg_df)
    mock_session.execute.assert_called()
    mock_session.commit.assert_called()


@patch("app.analytics.analytics.load_biometrics_data")
@patch("app.analytics.analytics.upsert_aggregates")
def test_analytics_aggregate_biometrics_success(
    mock_upsert, mock_load, sample_raw_data, capsys
):
    mock_load.return_value = sample_raw_data
    mock_upsert.return_value = 2

    analytics_aggregate_biometrics()

    captured = capsys.readouterr()
    assert "Starting analytics aggregation job" in captured.out
    assert "Aggregated and upserted 2 rows" in captured.out


@patch("app.analytics.analytics.load_biometrics_data")
def test_analytics_aggregate_biometrics_no_data(mock_load, capsys):
    mock_load.return_value = pd.DataFrame()

    analytics_aggregate_biometrics()

    captured = capsys.readouterr()
    assert "No data found in biometrics table" in captured.out
