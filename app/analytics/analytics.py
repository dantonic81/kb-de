import pandas as pd
from sqlalchemy import create_engine, text
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@db:5432/mydb")
engine = create_engine(DATABASE_URL)

def analytics_aggregate_biometrics():
    query = """
        SELECT
            patient_id,
            biometric_type,
            date_trunc('hour', timestamp) AS hour_start,
            value
        FROM biometrics
        WHERE biometric_type IN ('glucose', 'weight')
    """

    with engine.begin() as conn:
        df = pd.read_sql(query, conn)

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

    with engine.begin() as conn:
        for _, row in agg_df.iterrows():
            conn.execute(text("""
                INSERT INTO patient_biometric_hourly_summary (
                    patient_id, biometric_type, hour_start,
                    min_value, max_value, avg_value, count
                )
                VALUES (:patient_id, :biometric_type, :hour_start,
                        :min_value, :max_value, :avg_value, :count)
                ON CONFLICT (patient_id, biometric_type, hour_start)
                DO UPDATE SET
                    min_value = EXCLUDED.min_value,
                    max_value = EXCLUDED.max_value,
                    avg_value = EXCLUDED.avg_value,
                    count = EXCLUDED.count;
            """), row.to_dict())

    print(f"Aggregated and upserted {len(agg_df)} rows.")
