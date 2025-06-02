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
from contextlib import contextmanager
from typing import Dict, List, Tuple, Iterator, Any

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

_engine = None
_Session = None


def get_db_engine() -> Any:
    """Get the database engine instance, creating it if it doesn't exist.

    Returns:
        sqlalchemy.engine.Engine: The database engine instance
    """
    global _engine
    if _engine is None:
        _engine = create_engine(DATABASE_URL)
    return _engine


def get_sessionmaker() -> Any:
    """Get the SQLAlchemy sessionmaker, creating it if it doesn't exist.

    Returns:
        sqlalchemy.orm.sessionmaker: Configured sessionmaker instance
    """
    global _Session
    if _Session is None:
        _Session = sessionmaker(bind=get_db_engine())
    return _Session


@contextmanager
def get_db_session() -> Iterator[Any]:
    """Context manager to get a DB session.

    Yields:
        sqlalchemy.orm.Session: Database session

    Raises:
        Exception: Any exception that occurs during session usage
    """
    session_factory = get_sessionmaker()
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ---- Utility functions ----

def normalize_units(value: float, unit: str, metric_type: str) -> float:
    """Normalize units to standard values.

    Args:
        value: The value to convert
        unit: The original unit of the value
        metric_type: The type of metric being converted

    Returns:
        The converted value in standard units
    """
    if metric_type in UNIT_CONVERSIONS and unit in UNIT_CONVERSIONS[metric_type]:
        return UNIT_CONVERSIONS[metric_type][unit](value)
    return value


def validate_biometric_ranges(row: Dict[str, Any]) -> List[str]:
    """Validate biometric values against acceptable ranges.

    Args:
        row: Dictionary containing biometric data

    Returns:
        List of error messages for any validation failures
    """
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


# ---- Patient ETL ----

def load_patient_data(filepath: str) -> pd.DataFrame:
    """Load patient data from JSON file.

    Args:
        filepath: Path to the JSON file containing patient data

    Returns:
        DataFrame containing patient data or empty DataFrame if loading fails
    """
    try:
        return pd.read_json(filepath)
    except Exception as e:
        logger.error(f"Failed to load patient JSON: {e}")
        return pd.DataFrame()


def validate_patient_row(row: pd.Series) -> Tuple[bool, str]:
    """Validate a single patient record.

    Args:
        row: Pandas Series representing a patient record

    Returns:
        Tuple of (is_valid, error_message)
    """
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


def process_patients(filepath: str) -> None:
    """Process patient data from file and load into database.

    Args:
        filepath: Path to the patient data file
    """
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

    with get_db_session() as session:
        upsert_patients(session, valid_rows)
    save_invalid_patients(invalid_rows)


def upsert_patients(session: Any, records: List[Dict[str, Any]]) -> None:
    """Upsert patient records into the database.

    Args:
        session: Database session
        records: List of patient records to upsert

    Raises:
        Exception: If upsert operation fails
    """
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
        logger.info(f"Upserted {len(records)} patients")
    except Exception as e:
        logger.error(f"Failed to upsert patients: {e}")
        raise


def save_invalid_patients(invalid_rows: List[Dict[str, Any]]) -> None:
    """Save invalid patient records to a file.

    Args:
        invalid_rows: List of invalid patient records
    """
    if not invalid_rows:
        return
    os.makedirs("rejected", exist_ok=True)
    path = "rejected/patients_invalid.json"
    pd.DataFrame(invalid_rows).to_json(path, orient="records", indent=2)
    logger.info(f"Saved {len(invalid_rows)} invalid patient records to {path}")


# ---- Biometric ETL ----

def get_simulated_files() -> List[str]:
    """Get list of biometric data files sorted by timestamp.

    Returns:
        List of file paths sorted by their embedded timestamp
    """
    files = glob.glob(os.path.join(BIOMETRICS_DIR, f"{FILE_PREFIX}*{FILE_EXT}"))
    files.sort(key=lambda x: datetime.strptime(
        os.path.basename(x)[len(FILE_PREFIX):-len(FILE_EXT)],
        "%Y-%m-%dT%H-%M"
    ))
    return files


def read_biometric_chunks(csv_file: str) -> Tuple[Iterator[pd.DataFrame], List[Any]]:
    """Read biometric data file in chunks.

    Args:
        csv_file: Path to the CSV file to read

    Returns:
        Tuple of (chunk iterator, list of invalid rows)
    """
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


def validate_biometric_chunk(chunk: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Validate a chunk of biometric data.

    Args:
        chunk: DataFrame containing biometric records

    Returns:
        Tuple of (valid DataFrame, list of invalid rows as dicts)
    """
    try:
        BiometricSchema.validate(chunk, lazy=True)
        return chunk, []
    except pa.errors.SchemaErrors as err:
        invalid_indices = err.failure_cases['index'].dropna()
        valid_idx = set(chunk.index) - set(invalid_indices)
        valid_chunk = chunk.loc[list(valid_idx)]
        invalid_rows = chunk.loc[invalid_indices].to_dict("records")
        return valid_chunk, invalid_rows


def get_patients_map(session: Any, emails: List[str]) -> Dict[str, int]:
    """Get mapping of patient emails to their IDs.

    Args:
        session: Database session
        emails: List of patient emails to look up

    Returns:
        Dictionary mapping emails to patient IDs
    """
    patients = session.query(Patient).filter(Patient.email.in_(emails)).with_for_update().all()
    return {p.email: p.id for p in patients}


def process_biometric_records(chunk: pd.DataFrame, patients_map: Dict[str, int]) -> Tuple[
    List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Process and validate biometric records from a chunk.

    Args:
        chunk: DataFrame containing biometric records
        patients_map: Dictionary mapping emails to patient IDs

    Returns:
        Tuple of (list of valid records, list of invalid rows)
    """
    invalid_rows = []
    records = []

    missing_emails = set(chunk["patient_email"].unique()) - set(patients_map.keys())
    if missing_emails:
        logger.error(f"Missing patients for emails: {missing_emails}")
        chunk = chunk[~chunk["patient_email"].isin(missing_emails)]

    for _, row in chunk.iterrows():
        try:
            errors = validate_biometric_ranges(row.to_dict())
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


def upsert_biometric_records(session: Any, records: List[Dict[str, Any]]) -> None:
    """Upsert biometric records into the database.

    Args:
        session: Database session
        records: List of biometric records to upsert
    """
    try:
        session.bulk_insert_mappings(Biometric, records)
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
                session.commit()  # This is needed per record here to not lose changes
            except Exception as e:
                logger.error(f"Failed to upsert record: {e}")
                session.rollback()


def save_invalid_biometrics(invalid_rows: List[Dict[str, Any]]) -> None:
    """Save invalid biometric records to a file.

    Args:
        invalid_rows: List of invalid biometric records
    """
    if not invalid_rows:
        return
    os.makedirs("rejected", exist_ok=True)
    path = "rejected/biometrics_invalid.json"
    pd.DataFrame(invalid_rows).to_json(path, orient="records", indent=2)
    logger.info(f"Saved {len(invalid_rows)} invalid biometric records to {path}")


def process_biometrics() -> None:
    """Process all biometric files and load data into database."""
    all_invalid = []
    for file in get_simulated_files():
        logger.info(f"Processing biometric file: {file}")
        chunks, chunk_invalids = read_biometric_chunks(file)
        all_invalid.extend(chunk_invalids)

        with get_db_session() as session:
            for chunk in chunks:
                valid_chunk, invalid_rows = validate_biometric_chunk(chunk)
                all_invalid.extend(invalid_rows)

                emails = valid_chunk["patient_email"].unique().tolist()
                patients_map = get_patients_map(session, emails)

                records, invalids = process_biometric_records(valid_chunk, patients_map)
                all_invalid.extend(invalids)

                upsert_biometric_records(session, records)

    save_invalid_biometrics(all_invalid)


# ---- Main ETL ----

def run_etl() -> None:
    """Run the complete ETL process for patients and biometrics."""
    logger.info("Starting Patient ETL")
    process_patients(PATIENTS_FILE)
    logger.info("Patient ETL completed")

    logger.info("Starting Biometric ETL")
    process_biometrics()
    logger.info("Biometric ETL completed")


if __name__ == "__main__":
    run_etl()