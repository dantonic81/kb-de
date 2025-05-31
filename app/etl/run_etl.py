import os
import logging
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.db.models import Patient, Biometric
from app.schemas.patient_schema import PatientSchema
from app.schemas.biometric_schema import BiometricSchema

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@db:5432/mydb")
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

CHUNKSIZE = 1000


def load_patients(json_file='data/patients.json'):
    invalid_patient_rows = []
    valid_rows = []

    try:
        df = pd.read_json(json_file)
    except Exception as e:
        logger.error(f"Failed to load patient JSON: {e}")
        return

    # Iterate over raw rows first, validate before conversion
    for _, row in df.iterrows():
        row_dict = row.dropna().to_dict()  # drop NaN keys like 'foo'
        try:
            PatientSchema.validate(pd.DataFrame([row_dict]))
            valid_rows.append(row_dict)
        except Exception as e:
            logger.error(f"Patient row validation failed: {e} -- {row_dict}")
            row_with_error = row_dict.copy()
            row_with_error["validation_error"] = str(e)
            invalid_patient_rows.append(row_with_error)

    if not valid_rows:
        logger.warning("No valid patient records to process.")
        return

    # Now safely convert dob to date for the valid rows only
    df_valid = pd.DataFrame(valid_rows)
    df_valid["dob"] = pd.to_datetime(df_valid["dob"], errors="coerce").dt.date
    df_valid = df_valid.dropna(subset=["dob"])

    records = [
        {
            "email": row["email"],
            "name": row["name"],
            "dob": row["dob"],
            "gender": row.get("gender"),
            "address": row.get("address"),
            "phone": row.get("phone"),
            "sex": row.get("sex")
        }
        for _, row in df_valid.iterrows()
    ]

    stmt = pg_insert(Patient).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=["email"],
        set_={
            "name": stmt.excluded.name,
            "dob": stmt.excluded.dob,
            "gender": stmt.excluded.gender,
            "address": stmt.excluded.address,
            "phone": stmt.excluded.phone,
            "sex": stmt.excluded.sex
        }
    )

    try:
        session.execute(stmt)
        session.commit()
        logger.info(f"Upserted {len(records)} patients")
    except Exception as e:
        logger.error(f"Failed to upsert patients: {e}")
        session.rollback()

    if invalid_patient_rows:
        invalid_df = pd.DataFrame(invalid_patient_rows)
        invalid_df.to_json("rejected/patients_invalid.json", orient="records", indent=2)
        logger.info(f"Dumped {len(invalid_df)} invalid patient records to rejected/patients_invalid.json")


def load_biometrics(csv_file='data/biometrics.csv'):
    # NEW: Verify all pending operations are committed
    session.commit()
    session.expire_all()  # Clear all cached data to force fresh queries

    invalid_biometric_rows = []
    csv_columns = ["patient_email", "biometric_type", "value", "unit", "timestamp"]

    try:
        # NEW: Verify file exists first
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"Biometrics file not found: {csv_file}")

        chunks = pd.read_csv(csv_file, chunksize=CHUNKSIZE,
                             on_bad_lines=lambda bad: invalid_biometric_rows.append(bad) or None,
                             engine='python')
    except Exception as e:
        logger.error(f"Failed to load biometrics CSV: {e}")
        return

    for chunk in chunks:
        # NEW: Early validation for required columns
        missing_cols = set(csv_columns) - set(chunk.columns)
        if missing_cols:
            logger.error(f"Missing columns in CSV: {missing_cols}")
            continue

        chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors='coerce')
        valid_chunk = chunk.dropna(subset=["patient_email", "biometric_type", "unit", "timestamp", "value"])

        # NEW: Batch patient lookup with fresh query
        patient_emails = valid_chunk["patient_email"].unique().tolist()
        patients = {p.email: p.id for p in session.query(Patient)
        .filter(Patient.email.in_(patient_emails))
        .with_for_update()  # Lock rows to prevent race conditions
        .all()}

        # NEW: Track missing patients explicitly
        missing_emails = set(patient_emails) - set(patients.keys())
        if missing_emails:
            logger.error(f"Missing patients for emails: {missing_emails}")
            continue  # Or raise exception if this should be fatal

        records = []
        for _, row in valid_chunk.iterrows():
            try:
                patient_id = patients[row["patient_email"]]  # Now guaranteed to exist

                if row["biometric_type"] == "blood_pressure":
                    systolic, diastolic = map(int, row["value"].split("/"))
                    records.append({
                        "patient_id": patient_id,
                        "biometric_type": row["biometric_type"],
                        "systolic": systolic,
                        "diastolic": diastolic,
                        "value": None,
                        "unit": row["unit"],
                        "timestamp": row["timestamp"]
                    })
                else:
                    records.append({
                        "patient_id": patient_id,
                        "biometric_type": row["biometric_type"],
                        "value": float(row["value"]),
                        "unit": row["unit"],
                        "timestamp": row["timestamp"],
                        "systolic": None,
                        "diastolic": None
                    })
            except Exception as e:
                logger.error(f"Row processing failed: {e}\nRow: {row.to_dict()}")
                continue

        if records:
            try:
                # NEW: Explicitly check for foreign key violations
                stmt = pg_insert(Biometric).values(records)
                session.execute(stmt)
                session.commit()
                logger.info(f"Inserted {len(records)} biometrics")
            except IntegrityError as e:
                session.rollback()
                logger.critical(f"Data integrity error - possible corrupt patient references: {e}")
                raise  # Don't silently continue
            except Exception as e:
                session.rollback()
                logger.error(f"Unexpected error: {e}")

    if invalid_biometric_rows:
        pd.DataFrame(invalid_biometric_rows).to_csv("rejected/biometrics_invalid.csv", index=False)


def main():
    load_patients()
    load_biometrics()


if __name__ == "__main__":
    main()
