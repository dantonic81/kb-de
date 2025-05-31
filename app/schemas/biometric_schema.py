import pandera.pandas as pa
from pandera import Column, DataFrameSchema, Check
from datetime import datetime

BiometricSchema = DataFrameSchema({
    "patient_email": Column(str, nullable=False),
    "biometric_type": Column(str, nullable=False, checks=[
        Check.isin(["glucose", "weight", "blood_pressure"])
    ]),
    "value": Column(str, checks=[
        Check(lambda x: x.str.match(r'^(\d+\.?\d*|\d+\/\d+)$')),
    ]),
    "unit": Column(str, nullable=False),
    "timestamp": Column(pa.DateTime, nullable=False)
})