import pandas as pd
import logging
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from app.db.models import Patient, Biometric
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@db:5432/mydb")

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

def load_patients(json_file='data/patients.json'):
    try:
        df = pd.read_json(json_file)
    except Exception as e:
        logger.error(f"Failed to load patient JSON: {e}")
        return

    df = df.dropna(subset=["name", "dob"])

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

            session.add(patient)

        except Exception as e:
            logger.error(f"Error processing patient row {row.to_dict()}: {e}")

    session.commit()
    logger.info("Patients loaded successfully")


def load_biometrics(csv_file='data/biometrics.csv'):
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        logger.error(f"Failed to load biometrics CSV: {e}")
        return

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors='coerce')
    df = df.dropna(subset=["patient_email", "biometric_type", "unit", "timestamp", "value"])

    for _, row in df.iterrows():
        try:
            patient = session.query(Patient).filter_by(email=row["patient_email"]).first()
            if not patient:
                logger.warning(f"Patient not found: {row['patient_email']}, skipping row")
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
                        logger.info(f"Duplicate BP record found, skipping: {row.to_dict()}")
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
                    logger.info(f"Duplicate biometric found, skipping: {row.to_dict()}")
                    continue

                biometric = Biometric(
                    patient_id=patient.id,
                    biometric_type=biometric_type,
                    value=value,
                    unit=unit,
                    timestamp=timestamp
                )

            session.add(biometric)

        except Exception as e:
            logger.error(f"Error processing biometric row {row.to_dict()}: {e}")

    session.commit()
    logger.info("Biometrics loaded successfully")


def main():
    load_patients()
    load_biometrics()

if __name__ == "__main__":
    main()
