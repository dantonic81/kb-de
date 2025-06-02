import os
import pandas as pd
from typing import Dict
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from app.db.models import PatientBiometricHourlySummary

# Setup
DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://user:pass@db:5432/mydb")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def get_basic_biometrics_query() -> str:
    """
    Generate SQL query to retrieve non-null glucose and weight biometric data
    truncated to the hour level.

    Returns:
        str: SQL query string
    """
    return """
        SELECT
            patient_id,
            biometric_type,
            date_trunc('hour', timestamp) AS hour_start,
            value
        FROM biometrics
        WHERE biometric_type IN ('glucose', 'weight') AND value IS NOT NULL
    """

def get_blood_pressure_query() -> str:
    """
    Generate SQL query to retrieve non-null systolic and diastolic blood pressure data,
    each labeled with their own biometric_type, truncated to the hour level.

    Returns:
        str: SQL query string
    """
    return """
        SELECT
            patient_id,
            'blood_pressure_systolic' AS biometric_type,
            date_trunc('hour', timestamp) AS hour_start,
            systolic AS value
        FROM biometrics
        WHERE biometric_type = 'blood_pressure' AND systolic IS NOT NULL

        UNION ALL

        SELECT
            patient_id,
            'blood_pressure_diastolic' AS biometric_type,
            date_trunc('hour', timestamp) AS hour_start,
            diastolic AS value
        FROM biometrics
        WHERE biometric_type = 'blood_pressure' AND diastolic IS NOT NULL
    """

def load_biometrics_data() -> pd.DataFrame:
    """
    Load and combine biometric data (glucose, weight, blood pressure) from the database.

    Returns:
        pd.DataFrame: Combined biometric data with columns:
                      ['patient_id', 'biometric_type', 'hour_start', 'value']
    """
    with engine.begin() as conn:
        df_basic = pd.read_sql(get_basic_biometrics_query(), conn)
        df_bp = pd.read_sql(get_blood_pressure_query(), conn)

    combined_df = pd.concat([df_basic, df_bp], ignore_index=True)
    return combined_df

def aggregate_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate biometric data by patient, biometric type, and hour.

    Args:
        df (pd.DataFrame): Raw biometric data.

    Returns:
        pd.DataFrame: Aggregated hourly metrics including:
                      min_value, max_value, avg_value, and count.
    """
    agg_df = df.groupby(
        ['patient_id', 'biometric_type', 'hour_start']
    ).agg(
        min_value=('value', 'min'),
        max_value=('value', 'max'),
        avg_value=('value', 'mean'),
        count=('value', 'count')
    ).reset_index()

    agg_df['avg_value'] = agg_df['avg_value'].round(2)
    return agg_df

def upsert_aggregates(agg_df: pd.DataFrame) -> int:
    """
    Upsert aggregated biometric data into the summary table.

    Args:
        agg_df (pd.DataFrame): Aggregated biometric data.

    Returns:
        int: Number of records upserted.

    Raises:
        RuntimeError: If the upsert operation fails.
    """
    table = PatientBiometricHourlySummary.__table__
    records: list[Dict] = agg_df.to_dict(orient='records')

    stmt = insert(table).values(records)
    update_cols = {
        'min_value': stmt.excluded.min_value,
        'max_value': stmt.excluded.max_value,
        'avg_value': stmt.excluded.avg_value,
        'count': stmt.excluded.count
    }

    with Session() as session:
        try:
            session.execute(
                stmt.on_conflict_do_update(
                    index_elements=['patient_id', 'biometric_type', 'hour_start'],
                    set_=update_cols
                )
            )
            session.commit()
            return len(records)
        except SQLAlchemyError as e:
            session.rollback()
            raise RuntimeError(f"Upsert failed: {e}")

def analytics_aggregate_biometrics() -> None:
    """
    Main ETL function that:
    - Loads biometric data
    - Aggregates it hourly
    - Upserts it into a summary table
    """
    print("Starting analytics aggregation job...")

    df = load_biometrics_data()
    if df.empty:
        print("No data found in biometrics table.")
        return

    agg_df = aggregate_hourly(df)
    row_count = upsert_aggregates(agg_df)

    print(f"Aggregated and upserted {row_count} rows.")

if __name__ == "__main__":
    analytics_aggregate_biometrics()
