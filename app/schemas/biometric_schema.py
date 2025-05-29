import pandera as pa
from pandera import Column, DataFrameSchema, Check
from datetime import datetime

BiometricSchema = DataFrameSchema({
    "patient_email": Column(pa.String, nullable=False),
    "biometric_type": Column(pa.String, nullable=False),
    "value": Column(pa.String, nullable=False),  # Keep as string to support blood pressure like "120/80"
    "unit": Column(pa.String, nullable=True),
    "timestamp": Column(pa.String, nullable=False, checks=Check.str_matches(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$")),
})
