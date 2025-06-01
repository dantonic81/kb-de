import os
import logging
import glob
import pandas as pd
import pandera as pa
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from app.db.models import Patient, Biometric
from app.schemas.patient_schema import PatientSchema
from app.schemas.biometric_schema import BiometricSchema

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@db:5432/mydb")
BIOMETRICS_DIR = os.path.abspath(os.path.join(BASE_DIR, "..", "..", "biometrics_simulated"))
PATIENTS_FILE = os.path.join(BASE_DIR, "..", "..", "data", "patients.json")
FILE_PREFIX = "biometrics_"
FILE_EXT = ".csv"
CHUNKSIZE = 1000

# Biometric validation parameters
BIOMETRIC_RANGES = {
    "glucose": (70, 200),  # mg/dL
    "weight": (30, 200),  # kg
    "blood_pressure": {
        "systolic": (90, 140),
        "diastolic": (60, 90)
    }
}

UNIT_CONVERSIONS = {
    "weight": {
        "lbs": lambda x: x * 0.453592,
        "kg": lambda x: x
    }
}


def get_db_session():
    """Create and return a new database session"""
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    return Session()


def normalize_units(value: float, unit: str, metric_type: str) -> float:
    """Convert values to standard units"""
    if metric_type in UNIT_CONVERSIONS and unit in UNIT_CONVERSIONS[metric_type]:
        return UNIT_CONVERSIONS[metric_type][unit](value)
    return value


def validate_biometric_ranges(row: dict) -> list:
    """Check if values are within expected ranges"""
    errors = []
    metric_type = row["biometric_type"]

    if metric_type == "blood_pressure":
        try:
            systolic, diastolic = map(int, row["value"].split("/"))
            if not (BIOMETRIC_RANGES["blood_pressure"]["systolic"][0] <= systolic <=
                    BIOMETRIC_RANGES["blood_pressure"]["systolic"][1]):
                errors.append(f"Systolic BP {systolic} out of range")
            if not (BIOMETRIC_RANGES["blood_pressure"]["diastolic"][0] <= diastolic <=
                    BIOMETRIC_RANGES["blood_pressure"]["diastolic"][1]):
                errors.append(f"Diastolic BP {diastolic} out of range")
        except ValueError:
            errors.append("Invalid blood pressure format")
    else:
        try:
            value = float(row["value"])
            if metric_type in BIOMETRIC_RANGES:
                min_val, max_val = BIOMETRIC_RANGES[metric_type]
                if not (min_val <= value <= max_val):
                    errors.append(f"{metric_type} value {value} out of range")
        except ValueError:
            errors.append(f"Invalid value for {metric_type}")

    return errors


def load_patients(PATIENTS_FILE):
    """Load and process patient data"""
    invalid_patient_rows = []
    valid_rows = []

    try:
        df = pd.read_json(PATIENTS_FILE)
    except Exception as e:
        logger.error(f"Failed to load patient JSON: {e}")
        return

    # Validate each row
    for _, row in df.iterrows():
        row_dict = row.dropna().to_dict()
        try:
            PatientSchema.validate(pd.DataFrame([row_dict]))

            # Age validation
            dob = pd.to_datetime(row_dict["dob"]).date()
            age = (datetime.now().date() - dob).days / 365
            if age > 120 or age < 0:
                raise ValueError(f"Implausible age: {age:.1f} years")

            valid_rows.append(row_dict)
        except Exception as e:
            logger.error(f"Patient row validation failed: {e} -- {row_dict}")
            row_with_error = row_dict.copy()
            row_with_error["validation_error"] = str(e)
            invalid_patient_rows.append(row_with_error)

    if not valid_rows:
        logger.warning("No valid patient records to process.")
        return

    # Process valid rows
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

    session = get_db_session()
    try:
        # Upsert patients
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
        session.execute(stmt)
        session.commit()
        logger.info(f"Upserted {len(records)} patients")
    except Exception as e:
        logger.error(f"Failed to upsert patients: {e}")
        session.rollback()
    finally:
        session.close()

    # Save invalid records
    if invalid_patient_rows:
        os.makedirs("rejected", exist_ok=True)
        pd.DataFrame(invalid_patient_rows).to_json(
            "rejected/patients_invalid.json",
            orient="records",
            indent=2
        )
        logger.info(f"Saved {len(invalid_patient_rows)} invalid patient records")


def get_simulated_files():
    """Get all simulation files in chronological order"""
    files = glob.glob(os.path.join(BIOMETRICS_DIR, f"{FILE_PREFIX}*{FILE_EXT}"))
    # Sort files by timestamp in filename
    files.sort(key=lambda x: datetime.strptime(
        os.path.basename(x)[len(FILE_PREFIX):-len(FILE_EXT)],
        "%Y-%m-%dT%H-%M"
    ))
    return files


def process_biometric_file(csv_file: str):
    """Process a single biometric data file"""
    session = get_db_session()
    invalid_biometric_rows = []
    csv_columns = ["patient_email", "biometric_type", "value", "unit", "timestamp"]

    try:
        if not os.path.exists(csv_file):
            raise FileNotFoundError(f"Biometrics file not found: {csv_file}")

        chunks = pd.read_csv(csv_file, chunksize=CHUNKSIZE,
                             on_bad_lines=lambda bad: invalid_biometric_rows.append(bad) or None,
                             engine='python')
    except Exception as e:
        logger.error(f"Failed to load biometrics CSV: {e}")
        return

    for chunk in chunks:
        # Initial validation
        missing_cols = set(csv_columns) - set(chunk.columns)
        if missing_cols:
            logger.error(f"Missing columns in CSV: {missing_cols}")
            continue

        chunk["timestamp"] = pd.to_datetime(chunk["timestamp"], errors='coerce')
        valid_chunk = chunk.dropna(subset=csv_columns)

        # Schema validation
        try:
            BiometricSchema.validate(valid_chunk, lazy=True)
        except pa.errors.SchemaErrors as err:
            logger.warning(f"Schema validation errors: {err.failure_cases}")
            valid_idx = set(valid_chunk.index) - set(err.failure_cases['index'])
            valid_chunk = valid_chunk.loc[valid_idx]
            invalid_biometric_rows.extend(chunk.loc[err.failure_cases['index']].to_dict('records'))

        # Patient lookup
        patient_emails = valid_chunk["patient_email"].unique().tolist()
        patients = {p.email: p.id for p in session.query(Patient)
        .filter(Patient.email.in_(patient_emails))
        .with_for_update()
        .all()}

        missing_emails = set(patient_emails) - set(patients.keys())
        if missing_emails:
            logger.error(f"Missing patients for emails: {missing_emails}")
            valid_chunk = valid_chunk[~valid_chunk["patient_email"].isin(missing_emails)]

        # Process valid rows
        records = []
        for _, row in valid_chunk.iterrows():
            try:
                # Range validation
                range_errors = validate_biometric_ranges(row)
                if range_errors:
                    raise ValueError(", ".join(range_errors))

                record = {
                    "patient_id": patients[row["patient_email"]],
                    "biometric_type": row["biometric_type"],
                    "timestamp": row["timestamp"],
                    "unit": row["unit"]
                }

                if row["biometric_type"] == "blood_pressure":
                    systolic, diastolic = map(int, row["value"].split("/"))
                    record.update({
                        "systolic": systolic,
                        "diastolic": diastolic,
                        "value": None
                    })
                else:
                    value = float(row["value"])
                    if row["biometric_type"] == "weight":
                        value = normalize_units(value, row["unit"], "weight")
                        record["unit"] = "kg"
                    record.update({
                        "value": value,
                        "systolic": None,
                        "diastolic": None
                    })

                records.append(record)
            except Exception as e:
                logger.error(f"Row processing failed: {e}\nRow: {row.to_dict()}")
                invalid_biometric_rows.append(row.to_dict())

        # Batch upsert
        if records:
            stmt = pg_insert(Biometric).values(records)
            stmt = stmt.on_conflict_do_update(
                index_elements=[
                    'patient_id',
                    'biometric_type',
                    'timestamp',
                    text("COALESCE(value::text, '')"),
                    text("COALESCE(systolic::text, '')"),
                    text("COALESCE(diastolic::text, '')")
                ],
                set_={
                    "value": stmt.excluded.value,
                    "systolic": stmt.excluded.systolic,
                    "diastolic": stmt.excluded.diastolic,
                    "unit": stmt.excluded.unit
                }
            )

            try:
                session.execute(stmt)
                session.commit()
                logger.info(f"Processed {len(records)} biometric records from {csv_file}")
            except IntegrityError as e:
                session.rollback()
                logger.critical(f"Data integrity error: {e}")
                raise
            except Exception as e:
                session.rollback()
                logger.error(f"Unexpected error: {e}")

    # Save invalid records
    if invalid_biometric_rows:
        os.makedirs("rejected", exist_ok=True)
        invalid_file = os.path.join("rejected", f"invalid_{os.path.basename(csv_file)}")
        pd.DataFrame(invalid_biometric_rows).to_csv(invalid_file, index=False)
        logger.info(f"Saved {len(invalid_biometric_rows)} invalid biometric records to {invalid_file}")

    session.close()


def process_simulated_biometrics():
    """Process all available simulated files"""
    files = get_simulated_files()
    if not files:
        logger.info("No simulated files found")
        return

    for file in files:
        logger.info(f"Processing simulated file: {file}")
        process_biometric_file(file)


def main():
    """Run the ETL pipeline"""
    try:
        logger.info("Starting ETL pipeline")
        load_patients(PATIENTS_FILE)
        process_simulated_biometrics()
        logger.info("ETL pipeline completed successfully")
    except Exception as e:
        logger.critical(f"ETL pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()