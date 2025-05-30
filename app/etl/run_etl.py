import pandas as pd
import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
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
CHUNKSIZE = 1000  # Used only for biometrics

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
    patient_objects = []

    for _, row in df.iterrows():
        try:
            dob = pd.to_datetime(row["dob"]).date()
            patient = session.query(Patient).filter_by(email=row["email"]).first()

            if not patient:
                patient = Patient()

            patient.name = row["name"]
            patient.dob = dob
            patient.gender = row.get("gender")
            patient.address = row.get("address")
            patient.email = row.get("email")
            patient.phone = row.get("phone")
            patient.sex = row.get("sex")

            if USE_BULK_INSERT:
                patient_objects.append(patient)
            else:
                session.add(patient)

        except Exception as e:
            logger.error(f"Error processing patient row {row.to_dict()}: {e}")

    if USE_BULK_INSERT:
        bulk_insert_objects(patient_objects, "patients")
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
        logger.info(f"Processing new biometric chunk ({len(chunk)} rows)")
        chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors='coerce')
        chunk = chunk.dropna(subset=["patient_email", "biometric_type", "unit", "timestamp", "value"])
        chunk["timestamp"] = chunk["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")

        try:
            chunk = BiometricSchema.validate(chunk)
        except Exception as e:
            logger.error(f"Biometric data validation failed:\n{e}")
            continue

        chunk["timestamp"] = pd.to_datetime(chunk["timestamp"])
        biometrics = []

        for _, row in chunk.iterrows():
            try:
                patient = session.query(Patient).filter_by(email=row["patient_email"]).first()
                if not patient:
                    logger.warning(f"Patient not found: {row['patient_email']}, skipping")
                    continue

                biometric_type = row["biometric_type"]
                unit = row["unit"]
                timestamp = row["timestamp"]

                if biometric_type == "blood_pressure":
                    try:
                        systolic_str, diastolic_str = row["value"].split("/")
                        systolic = int(systolic_str)
                        diastolic = int(diastolic_str)

                        exists = session.query(Biometric).filter_by(
                            patient_id=patient.id,
                            biometric_type=biometric_type,
                            systolic=systolic,
                            diastolic=diastolic,
                            unit=unit,
                            timestamp=timestamp
                        ).first()
                        if exists:
                            continue

                        biometric = Biometric(
                            patient_id=patient.id,
                            biometric_type=biometric_type,
                            systolic=systolic,
                            diastolic=diastolic,
                            unit=unit,
                            timestamp=timestamp
                        )
                    except Exception as e:
                        logger.error(f"Invalid BP value in row {row.to_dict()}: {e}")
                        continue

                else:
                    value = float(row["value"])

                    exists = session.query(Biometric).filter_by(
                        patient_id=patient.id,
                        biometric_type=biometric_type,
                        value=value,
                        unit=unit,
                        timestamp=timestamp
                    ).first()
                    if exists:
                        continue

                    biometric = Biometric(
                        patient_id=patient.id,
                        biometric_type=biometric_type,
                        value=value,
                        unit=unit,
                        timestamp=timestamp
                    )

                if USE_BULK_INSERT:
                    biometrics.append(biometric)
                else:
                    session.add(biometric)

            except Exception as e:
                logger.error(f"Error processing biometric row {row.to_dict()}: {e}")

        if USE_BULK_INSERT:
            bulk_insert_objects(biometrics, "biometrics")
        else:
            session.commit()
            logger.info("Biometrics chunk inserted individually")

def main():
    load_patients()
    load_biometrics()

if __name__ == "__main__":
    main()
