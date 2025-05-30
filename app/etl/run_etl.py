import pandas as pd
import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, and_, or_
from app.db.models import Patient, Biometric
import os
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

USE_BULK_INSERT = True
CHUNKSIZE = 1000

def bulk_insert_objects(objects, model_name=""):
    try:
        session.bulk_save_objects(objects)
        session.commit()
        logger.info(f"Bulk inserted {len(objects)} {model_name}")
    except Exception as e:
        logger.error(f"Bulk insert failed for {model_name}: {e}")
        session.rollback()

def load_patients(json_file='data/patients.json'):
    try:
        df = pd.read_json(json_file)
        df = PatientSchema.validate(df)
    except Exception as e:
        logger.error(f"Error loading or validating patient data: {e}")
        return

    df = df.dropna(subset=["name", "dob"])
    emails = df["email"].dropna().unique().tolist()

    existing_patients = {
        p.email: p for p in session.query(Patient).filter(Patient.email.in_(emails)).all()
    }

    new_patients = []

    for _, row in df.iterrows():
        email = row.get("email")
        if not email:
            continue

        dob = pd.to_datetime(row["dob"]).date()
        patient = existing_patients.get(email)

        if not patient:
            patient = Patient(email=email)
        patient.name = row["name"]
        patient.dob = dob
        patient.gender = row.get("gender")
        patient.address = row.get("address")
        patient.phone = row.get("phone")
        patient.sex = row.get("sex")

        if USE_BULK_INSERT:
            new_patients.append(patient)
        else:
            session.merge(patient)  # merge avoids duplication

    if USE_BULK_INSERT:
        bulk_insert_objects(new_patients, "patients")
    else:
        session.commit()
        logger.info("Patients inserted individually")

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
        chunk["timestamp"] = chunk["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")

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

        # Build composite keys from chunk
        keys = []
        for _, row in chunk.iterrows():
            patient_id = patients.get(row["patient_email"])
            if not patient_id:
                continue
            if row["biometric_type"] == "blood_pressure":
                try:
                    systolic, diastolic = map(int, row["value"].split("/"))
                    keys.append((
                        patient_id,
                        row["biometric_type"],
                        systolic,
                        diastolic,
                        None,
                        row["unit"],
                        row["timestamp"]
                    ))
                except:
                    continue
            else:
                keys.append((
                    patient_id,
                    row["biometric_type"],
                    None,
                    None,
                    float(row["value"]),
                    row["unit"],
                    row["timestamp"]
                ))

        # Fetch duplicates in one query
        conditions = []
        for k in keys:
            cond = and_(
                Biometric.patient_id == k[0],
                Biometric.biometric_type == k[1],
                Biometric.systolic == k[2],
                Biometric.diastolic == k[3],
                Biometric.value == k[4],
                Biometric.unit == k[5],
                Biometric.timestamp == k[6],
            )
            conditions.append(cond)

        existing = set()
        if conditions:
            existing = {
                (
                    b.patient_id,
                    b.biometric_type,
                    b.systolic,
                    b.diastolic,
                    b.value,
                    b.unit,
                    b.timestamp
                )
                for b in session.query(Biometric).filter(or_(*conditions)).all()
            }

        new_biometrics = []

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
                    key = (patient_id, biometric_type, systolic, diastolic, None, unit, timestamp)
                    if key in existing:
                        continue
                    biometric = Biometric(
                        patient_id=patient_id,
                        biometric_type=biometric_type,
                        systolic=systolic,
                        diastolic=diastolic,
                        unit=unit,
                        timestamp=timestamp
                    )
                except Exception as e:
                    logger.error(f"Invalid BP value: {row['value']} – {e}")
                    continue
            else:
                try:
                    value = float(row["value"])
                    key = (patient_id, biometric_type, None, None, value, unit, timestamp)
                    if key in existing:
                        continue
                    biometric = Biometric(
                        patient_id=patient_id,
                        biometric_type=biometric_type,
                        value=value,
                        unit=unit,
                        timestamp=timestamp
                    )
                except Exception as e:
                    logger.error(f"Invalid value: {row['value']} – {e}")
                    continue

            new_biometrics.append(biometric)

        if USE_BULK_INSERT:
            bulk_insert_objects(new_biometrics, "biometrics")
        else:
            for b in new_biometrics:
                session.add(b)
            session.commit()
            logger.info("Inserted biometric chunk")

def main():
    load_patients()
    load_biometrics()

if __name__ == "__main__":
    main()
