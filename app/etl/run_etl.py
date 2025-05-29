import json
import csv
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
    with open(json_file) as f:
        patients_data = json.load(f)
    for p in patients_data:
        # Simple validation
        if not p.get('name') or not p.get('dob'):
            logger.warning(f"Skipping invalid patient record: {p}")
            continue

        try:
            dob = datetime.strptime(p['dob'], "%Y-%m-%d").date()
        except Exception as e:
            logger.error(f"Invalid DOB format for patient {p['name']}: {e}")
            continue

        # Upsert patient
        patient = session.query(Patient).filter_by(email=p['email']).first()
        if not patient:
            patient = Patient()
        patient.name = p['name']
        patient.dob = dob
        patient.gender = p.get('gender')
        patient.address = p.get('address')
        patient.email = p.get('email')
        patient.phone = p.get('phone')
        patient.sex = p.get('sex')
        session.add(patient)
    session.commit()
    logger.info("Patients loaded successfully")

def load_biometrics(csv_file='data/biometrics.csv'):
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                patient_email = row['patient_email']
                patient = session.query(Patient).filter_by(email=patient_email).first()
                if not patient:
                    logger.error(f"Patient not found for email {patient_email}, skipping row: {row}")
                    continue

                biometric_type = row['biometric_type']
                unit = row['unit']
                timestamp = datetime.strptime(row['timestamp'], "%Y-%m-%dT%H:%M:%S")

                if biometric_type == "blood_pressure":
                    try:
                        systolic_str, diastolic_str = row["value"].split("/")
                        systolic = int(systolic_str)
                        diastolic = int(diastolic_str)

                        # Check for duplicate BP record
                        exists = session.query(Biometric).filter_by(
                            patient_id=patient.id,
                            biometric_type=biometric_type,
                            systolic=systolic,
                            diastolic=diastolic,
                            unit=unit,
                            timestamp=timestamp
                        ).first()
                        if exists:
                            logger.info(f"Duplicate blood pressure record found, skipping: {row}")
                            continue

                        biometric = Biometric(
                            patient_id=patient.id,
                            biometric_type=biometric_type,
                            systolic=systolic,
                            diastolic=diastolic,
                            unit=unit,
                            timestamp=timestamp
                        )
                    except Exception as bp_error:
                        logger.error(f"Value error in blood pressure row {row}: {bp_error}")
                        continue
                else:
                    value = float(row['value'])

                    # Check for duplicate general biometric record
                    exists = session.query(Biometric).filter_by(
                        patient_id=patient.id,
                        biometric_type=biometric_type,
                        value=value,
                        unit=unit,
                        timestamp=timestamp
                    ).first()
                    if exists:
                        logger.info(f"Duplicate biometric record found, skipping: {row}")
                        continue

                    biometric = Biometric(
                        patient_id=patient.id,
                        biometric_type=biometric_type,
                        value=value,
                        unit=unit,
                        timestamp=timestamp
                    )

                session.add(biometric)

            except ValueError as ve:
                logger.error(f"Value error in row {row}: {ve}")
            except Exception as e:
                logger.error(f"Skipping invalid biometric row {row}: {e}")

        session.commit()
        logger.info("Biometrics loaded successfully")


def main():
    load_patients()
    load_biometrics()

if __name__ == "__main__":
    main()
