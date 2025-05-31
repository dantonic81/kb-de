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
    invalid_biometric_rows = []

    csv_columns = ["patient_email", "biometric_type", "value", "unit", "timestamp"]
    # Custom handler for bad lines:
    def bad_line_handler(bad_line: list):
        row_dict = {csv_columns[i] if i < len(csv_columns) else f"col_{i}": val for i, val in enumerate(bad_line)}
        invalid_biometric_rows.append(row_dict)
        logger.error(f"Malformed CSV row: {bad_line}")
        return None

    try:
        chunks = pd.read_csv(csv_file, chunksize=CHUNKSIZE, on_bad_lines=bad_line_handler, engine='python')
    except Exception as e:
        logger.error(f"Failed to load biometrics CSV: {e}")
        return

    for chunk in chunks:
        logger.info(f"Processing biometric chunk ({len(chunk)} rows)")

        chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors='coerce')
        chunk = chunk.dropna(subset=["patient_email", "biometric_type", "unit", "timestamp", "value"])

        valid_rows = []

        for _, row in chunk.iterrows():
            try:
                BiometricSchema.validate(pd.DataFrame([row]))
                valid_rows.append(row)
            except Exception as e:
                logger.error(f"Biometric row validation failed: {e} -- {row.to_dict()}")
                row_with_error = row.copy()
                row_with_error["validation_error"] = str(e)
                invalid_biometric_rows.append(row_with_error)

        if not valid_rows:
            logger.info("No valid records to upsert for this chunk.")
            continue

        valid_chunk = pd.DataFrame(valid_rows)
        patient_emails = valid_chunk["patient_email"].unique().tolist()

        patients = {
            p.email: p.id for p in session.query(Patient).filter(Patient.email.in_(patient_emails)).all()
        }

        records = []

        for _, row in valid_chunk.iterrows():
            patient_id = patients.get(row["patient_email"])
            if not patient_id:
                logger.warning(f"No patient found for email: {row['patient_email']}")
                continue

            biometric_type = row["biometric_type"]
            unit = row["unit"]
            timestamp = row["timestamp"]

            if biometric_type == "blood_pressure":
                try:
                    systolic, diastolic = map(int, row["value"].split("/"))
                    record = {
                        "patient_id": patient_id,
                        "biometric_type": biometric_type,
                        "systolic": systolic,
                        "diastolic": diastolic,
                        "value": None,
                        "unit": unit,
                        "timestamp": timestamp
                    }
                except Exception as e:
                    logger.error(f"Invalid BP value: {row['value']} – {e}")
                    continue
            else:
                try:
                    value = float(row["value"])
                    record = {
                        "patient_id": patient_id,
                        "biometric_type": biometric_type,
                        "systolic": None,
                        "diastolic": None,
                        "value": value,
                        "unit": unit,
                        "timestamp": timestamp
                    }
                except Exception as e:
                    logger.error(f"Invalid value: {row['value']} – {e}")
                    continue

            records.append(record)

        if not records:
            logger.info("No valid records to upsert for this chunk.")
            continue

        stmt = pg_insert(Biometric).values(records)
        stmt = stmt.on_conflict_do_nothing(constraint="unique_biometric_entry")

        try:
            session.execute(stmt)
            session.commit()
            logger.info(f"Upserted {len(records)} biometrics")
        except Exception as e:
            logger.error(f"Failed to upsert biometrics: {e}")
            session.rollback()

    if invalid_biometric_rows:
        invalid_df = pd.DataFrame(invalid_biometric_rows)
        invalid_df.to_csv("rejected/biometrics_invalid.csv", index=False)
        logger.info(f"Dumped {len(invalid_df)} invalid biometric records to rejected/biometrics_invalid.csv")


def main():
    load_patients()
    load_biometrics()


if __name__ == "__main__":
    main()
