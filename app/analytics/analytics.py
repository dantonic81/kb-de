import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@db:5432/mydb")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# Assuming you have your ORM model defined somewhere like this:
from app.db.models import PatientBiometricHourlySummary  # adjust import path accordingly

def analytics_aggregate_biometrics():
    query_basic = """
        SELECT
            patient_id,
            biometric_type,
            date_trunc('hour', timestamp) AS hour_start,
            value
        FROM biometrics
        WHERE biometric_type IN ('glucose', 'weight')
    """

    query_bp = """
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

    with engine.begin() as conn:
        df_basic = pd.read_sql(query_basic, conn)
        df_bp = pd.read_sql(query_bp, conn)

    df = pd.concat([df_basic, df_bp], ignore_index=True)

    if df.empty:
        print("No data found in biometrics table.")
        return

    agg_df = df.groupby(
        ['patient_id', 'biometric_type', 'hour_start']
    ).agg(
        min_value=('value', 'min'),
        max_value=('value', 'max'),
        avg_value=('value', 'mean'),
        count=('value', 'count')
    ).reset_index()

    # Batch upsert using SQLAlchemy ORM session and insert().on_conflict_do_update()
    with Session() as session:
        table = PatientBiometricHourlySummary.__table__
        records = agg_df.to_dict(orient='records')

        stmt = insert(table).values(records)

        update_cols = {
            'min_value': stmt.excluded.min_value,
            'max_value': stmt.excluded.max_value,
            'avg_value': stmt.excluded.avg_value,
            'count': stmt.excluded.count
        }

        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=['patient_id', 'biometric_type', 'hour_start'],
            set_=update_cols
        )
        session.execute(upsert_stmt)
        session.commit()

    print(f"Aggregated and upserted {len(agg_df)} rows.")

if __name__ == "__main__":
    analytics_aggregate_biometrics()
