import os
import logging
import glob
import pandas as pd
import pandera as pa
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from app.db.models import Patient, Biometric
from app.schemas.patient_schema import PatientSchema
from app.schemas.biometric_schema import BiometricSchema

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
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

BIOMETRIC_RANGES = {
    "glucose": (70, 200),
    "weight": (30, 200),
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


# ---- Database ----

def get_db_engine():
    return create_engine(DATABASE_URL)

def get_db_session():
    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    return Session()


# ---- Utility functions ----

def normalize_units(value: float, unit: str, metric_type: str) -> float:
    if metric_type in UNIT_CONVERSIONS and unit in UNIT_CONVERSIONS[metric_type]:
        return UNIT_CONVERSIONS[metric_type][unit](value)
    return value

def validate_biometric_ranges(row: dict) -> list:
    errors = []
    metric_type = row["biometric_type"]

    if metric_type == "blood_pressure":
        try:
            systolic, diastolic = map(int, row["value"].split("/"))
            if not (BIOMETRIC_RANGES["blood_pressure"]["systolic"][0] <= systolic <= BIOMETRIC_RANGES["blood_pressure"]["systolic"][1]):
                errors.append(f"Systolic BP {systolic} out of range")
            if not (BIOMETRIC_RANGES["blood_pressure"]["diastolic"][0] <= diastolic <= BIOMETRIC_RANGES["blood_pressure"]["diastolic"][1]):
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


# ---- Patient ETL ----

def load_patient_data(filepath: str) -> pd.DataFrame:
    try:
        return pd.read_json(filepath)
    except Exception as e:
        logger.error(f"Failed to load patient JSON: {e}")
        return pd.DataFrame()

def validate_patient_row(row: pd.Series) -> (bool, str):
    row_dict = row.dropna().to_dict()
    try:
        PatientSchema.validate(pd.DataFrame([row_dict]))
        dob = pd.to_datetime(row_dict["dob"]).date()
        age = (datetime.now().date() - dob).days / 365
        if age > 120 or age < 0:
            return False, f"Implausible age: {age:.1f} years"
        return True, ""
    except Exception as e:
        return False, str(e)

def process_patients(filepath: str):
    df = load_patient_data(filepath)
    if df.empty:
        logger.warning("No patient data loaded.")
        return

    valid_rows, invalid_rows = [], []
    for _, row in df.iterrows():
        is_valid, err = validate_patient_row(row)
        if is_valid:
            valid_rows.append(row.dropna().to_dict())
        else:
            row_dict = row.dropna().to_dict()
            row_dict["validation_error"] = err
            invalid_rows.append(row_dict)

    if not valid_rows:
        logger.warning("No valid patient records found.")
        return

    upsert_patients(valid_rows)
    save_invalid_patients(invalid_rows)

def upsert_patients(records: list):
    session = get_db_session()
    # Normalize dob to date and drop nulls
    for rec in records:
        try:
            rec["dob"] = pd.to_datetime(rec["dob"]).date()
        except Exception:
            rec["dob"] = None

    try:
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

def save_invalid_patients(invalid_rows: list):
    if not invalid_rows:
        return
    os.makedirs("rejected", exist_ok=True)
    path = "rejected/patients_invalid.json"
    pd.DataFrame(invalid_rows).to_json(path, orient="records", indent=2)
    logger.info(f"Saved {len(invalid_rows)} invalid patient records to {path}")


# ---- Biometric ETL ----

def get_simulated_files():
    files = glob.glob(os.path.join(BIOMETRICS_DIR, f"{FILE_PREFIX}*{FILE_EXT}"))
    files.sort(key=lambda x: datetime.strptime(
        os.path.basename(x)[len(FILE_PREFIX):-len(FILE_EXT)],
        "%Y-%m-%dT%H-%M"
    ))
    return files

def read_biometric_chunks(csv_file: str):
    invalid_rows = []
    try:
        chunks = pd.read_csv(
            csv_file,
            chunksize=CHUNKSIZE,
            on_bad_lines=lambda bad: invalid_rows.append(bad) or None,
            engine='python'
        )
        return chunks, invalid_rows
    except Exception as e:
        logger.error(f"Failed to load biometrics CSV {csv_file}: {e}")
        return [], []

def validate_biometric_chunk(chunk: pd.DataFrame):
    try:
        BiometricSchema.validate(chunk, lazy=True)
        return chunk, []
    except pa.errors.SchemaErrors as err:
        invalid_indices = err.failure_cases['index'].dropna()
        valid_idx = set(chunk.index) - set(invalid_indices)
        valid_chunk = chunk.loc[list(valid_idx)]
        invalid_rows = chunk.loc[invalid_indices].to_dict("records")
        return valid_chunk, invalid_rows

def get_patients_map(session, emails: list):
    patients = session.query(Patient).filter(Patient.email.in_(emails)).with_for_update().all()
    return {p.email: p.id for p in patients}

def process_biometric_records(session, chunk: pd.DataFrame, patients_map: dict):
    invalid_rows = []
    records = []

    missing_emails = set(chunk["patient_email"].unique()) - set(patients_map.keys())
    if missing_emails:
        logger.error(f"Missing patients for emails: {missing_emails}")
        chunk = chunk[~chunk["patient_email"].isin(missing_emails)]

    for _, row in chunk.iterrows():
        try:
            errors = validate_biometric_ranges(row)
            if errors:
                raise ValueError(", ".join(errors))

            rec = {
                "patient_id": patients_map[row["patient_email"]],
                "biometric_type": row["biometric_type"],
                "timestamp": row["timestamp"],
                "unit": row["unit"]
            }

            if row["biometric_type"] == "blood_pressure":
                systolic, diastolic = map(int, row["value"].split("/"))
                rec.update({"systolic": systolic, "diastolic": diastolic, "value": None})
            else:
                value = float(row["value"])
                if row["biometric_type"] == "weight":
                    value = normalize_units(value, row["unit"], "weight")
                    rec["unit"] = "kg"
                rec.update({"value": value, "systolic": None, "diastolic": None})

            records.append(rec)
        except Exception as e:
            logger.error(f"Row processing failed: {e}\nRow: {row.to_dict()}")
            invalid_rows.append(row.to_dict())

    return records, invalid_rows

def upsert_biometric_records(session, records):
    try:
        session.bulk_insert_mappings(Biometric, records)
        session.commit()
        logger.info(f"Inserted {len(records)} biometric records")
    except IntegrityError:
        session.rollback()
        logger.info("Duplicate detected, switching to individual upserts")
        for rec in records:
            try:
                stmt = pg_insert(Biometric).values(rec)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['patient_id', 'biometric_type', 'timestamp'],
                    set_={
                        "value": stmt.excluded.value,
                        "systolic": stmt.excluded.systolic,
                        "diastolic": stmt.excluded.diastolic,
                        "unit": stmt.excluded.unit
                    }
                )
                session.execute(stmt)
                session.commit()
            except Exception as e:
                logger.error(f"Failed to upsert record: {e}")
                session.rollback()

def save_invalid_biometrics(invalid_rows: list):
    if not invalid_rows:
        return
    os.makedirs("rejected", exist_ok=True)
    path = "rejected/biometrics_invalid.json"
    pd.DataFrame(invalid_rows).to_json(path, orient="records", indent=2)
    logger.info(f"Saved {len(invalid_rows)} invalid biometric records to {path}")

def process_biometrics():
    session = get_db_session()
    all_invalid = []
    for file in get_simulated_files():
        logger.info(f"Processing biometric file: {file}")
        chunks, chunk_invalids = read_biometric_chunks(file)
        all_invalid.extend(chunk_invalids)

        for chunk in chunks:
            valid_chunk, invalid_chunk = validate_biometric_chunk(chunk)
            all_invalid.extend(invalid_chunk)

            emails = valid_chunk["patient_email"].unique().tolist()
            patients_map = get_patients_map(session, emails)
            records, invalid_rows = process_biometric_records(session, valid_chunk, patients_map)
            all_invalid.extend(invalid_rows)

            upsert_biometric_records(session, records)
    save_invalid_biometrics(all_invalid)
    session.close()


# ---- Main runner ----

def main():
    logger.info("Starting ETL process")
    process_patients(PATIENTS_FILE)
    process_biometrics()
    logger.info("ETL process completed")

if __name__ == "__main__":
    main()
