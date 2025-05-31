import pandas as pd
from sqlalchemy import create_engine, text
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@db:5432/mydb")
engine = create_engine(DATABASE_URL)

def analytics_aggregate_biometrics():
    # Query glucose and weight first, since they use 'value'
    query_basic = """
        SELECT
            patient_id,
            biometric_type,
            date_trunc('hour', timestamp) AS hour_start,
            value
        FROM biometrics
        WHERE biometric_type IN ('glucose', 'weight')
    """

    # Query blood pressure separately, getting systolic and diastolic as separate rows
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

    # Combine all biometrics into one dataframe
    df = pd.concat([df_basic, df_bp], ignore_index=True)

    if df.empty:
        print("No data found in biometrics table.")
        return

    # Group by patient, biometric type, and hour, then aggregate
    agg_df = df.groupby(
        ['patient_id', 'biometric_type', 'hour_start']
    ).agg(
        min_value=('value', 'min'),
        max_value=('value', 'max'),
        avg_value=('value', 'mean'),
        count=('value', 'count')
    ).reset_index()

    # Upsert the aggregated data into patient_biometric_hourly_summary
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
