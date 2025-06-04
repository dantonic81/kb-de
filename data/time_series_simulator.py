import csv
import os
from datetime import datetime, timedelta
import random
import json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PATIENTS_FILE = os.path.join(BASE_DIR, "patients.json")
BIOMETRICS_DIR = os.path.join(BASE_DIR, "../biometrics_simulated")
FILE_PREFIX = "biometrics_"
FILE_EXT = ".csv"


# Helper functions to generate realistic biometric values
def generate_glucose():
    # Typical fasting glucose range 70-130 mg/dL
    return random.randint(80, 130)


def generate_weight():
    # Weight range 50-90 kg for simulation
    return round(random.uniform(50, 90), 1)


def generate_blood_pressure():
    # Systolic 110-140, Diastolic 70-90
    sys = random.randint(110, 140)
    dia = random.randint(70, 90)
    return f"{sys}/{dia}"


def load_patients():
    with open(PATIENTS_FILE, "r") as f:
        return json.load(f)


def find_latest_timestamp():
    """Find the latest timestamp from existing simulated files"""
    if not os.path.exists(BIOMETRICS_DIR):
        os.makedirs(BIOMETRICS_DIR)
        return None
    files = [
        f
        for f in os.listdir(BIOMETRICS_DIR)
        if f.startswith(FILE_PREFIX) and f.endswith(FILE_EXT)
    ]
    if not files:
        return None
    # filenames look like biometrics_2025-06-01T10.csv
    timestamps = []
    for f in files:
        try:
            ts_str = f[len(FILE_PREFIX): -len(FILE_EXT)]  # extract datetime string
            ts = datetime.fromisoformat(ts_str)
            timestamps.append(ts)
        except Exception:
            continue
    if timestamps:
        return max(timestamps)
    return None


def simulate_and_write():
    patients = load_patients()
    latest_ts = find_latest_timestamp()
    if latest_ts is None:
        latest_ts = datetime.now() - timedelta(hours=1)
    new_ts = latest_ts + timedelta(hours=1)

    filename = f"{FILE_PREFIX}{new_ts.strftime('%Y-%m-%dT%H-%M')}{FILE_EXT}"
    filepath = os.path.join(BIOMETRICS_DIR, filename)

    if os.path.exists(filepath):
        print(f"File {filename} already exists. Exiting to avoid duplicates.")
        return

    print(
        f"Generating simulated biometrics data for time: {new_ts.strftime('%Y-%m-%d %H:%M')}"
    )
    with open(filepath, "w", newline="") as csvfile:
        fieldnames = ["patient_email", "biometric_type", "value", "unit", "timestamp"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for patient in patients:
            email = patient["email"]
            ts_str = new_ts.strftime("%Y-%m-%dT%H:%M:%S")

            writer.writerow(
                {
                    "patient_email": email,
                    "biometric_type": "glucose",
                    "value": generate_glucose(),
                    "unit": "mg/dL",
                    "timestamp": ts_str,
                }
            )

            # Generate weight reading
            writer.writerow(
                {
                    "patient_email": email,
                    "biometric_type": "weight",
                    "value": generate_weight(),
                    "unit": "kg",
                    "timestamp": ts_str,
                }
            )

            # Generate blood pressure reading
            writer.writerow(
                {
                    "patient_email": email,
                    "biometric_type": "blood_pressure",
                    "value": generate_blood_pressure(),
                    "unit": "mmHg",
                    "timestamp": ts_str,
                }
            )

    print(f"Simulation file written: {filepath}")


if __name__ == "__main__":
    simulate_and_write()
