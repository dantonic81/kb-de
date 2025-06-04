from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class PatientBiometricHourlySummaryETL(BaseModel):
    patient_id: int
    biometric_type: str
    hour_start: datetime
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    avg_value: Optional[float] = None
    count: Optional[int] = None
