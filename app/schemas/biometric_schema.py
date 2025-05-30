import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check
from datetime import datetime

BiometricSchema = DataFrameSchema({
    "patient_email": Column(pa.String, nullable=False),
    "biometric_type": Column(pa.String, nullable=False),
    "value": Column(pa.String, nullable=False),  # Keep as string to support blood pressure like "120/80"
    "unit": Column(pa.String, nullable=True),
    "timestamp": Column(pa.DateTime, nullable=False), # Expect datetime object
})
