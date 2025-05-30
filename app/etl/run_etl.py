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
    try:
        df = pd.read_json(json_file)
        df = PatientSchema.validate(df)
    except Exception as e:
        logger.error(f"Error loading or validating patient data: {e}")
        return

    df = df.dropna(subset=["name", "dob"])
    df["dob"] = pd.to_datetime(df["dob"]).dt.date
    df = df.dropna(subset=["email"])

    records = []
    for _, row in df.iterrows():
        records.append({
            "email": row["email"],
            "name": row["name"],
            "dob": row["dob"],
            "gender": row.get("gender"),
            "address": row.get("address"),
            "phone": row.get("phone"),
            "sex": row.get("sex")
        })

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


def load_biometrics(csv_file='data/biometrics.csv'):
    try:
        chunks = pd.read_csv(csv_file, chunksize=CHUNKSIZE)
    except Exception as e:
        logger.error(f"Failed to load biometrics CSV: {e}")
        return

    for chunk in chunks:
        logger.info(f"Processing biometric chunk ({len(chunk)} rows)")

        chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors='coerce')
        chunk = chunk.dropna(subset=["patient_email", "biometric_type", "unit", "timestamp", "value"])

        try:
            chunk = BiometricSchema.validate(chunk)
        except Exception as e:
            logger.error(f"Validation failed:\n{e}")
            continue

        chunk["timestamp"] = pd.to_datetime(chunk["timestamp"])
        patient_emails = chunk["patient_email"].unique().tolist()

        patients = {
            p.email: p.id for p in session.query(Patient).filter(Patient.email.in_(patient_emails)).all()
        }

        records = []

        for _, row in chunk.iterrows():
            patient_id = patients.get(row["patient_email"])
            if not patient_id:
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
            continue

        stmt = pg_insert(Biometric).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=[
            "patient_id", "biometric_type", "systolic", "diastolic", "value", "unit", "timestamp"
        ])

        try:
            session.execute(stmt)
            session.commit()
            logger.info(f"Upserted {len(records)} biometrics")
        except Exception as e:
            logger.error(f"Failed to upsert biometrics: {e}")
            session.rollback()


def main():
    load_patients()
    load_biometrics()


if __name__ == "__main__":
    main()
