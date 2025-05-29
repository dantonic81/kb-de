from pydantic import BaseModel
from datetime import datetime

class BiometricIn(BaseModel):
    biometric_type: str
    value: float
    unit: str
    timestamp: datetime

class BiometricOut(BiometricIn):
    id: int
    patient_id: int

    class Config:
        orm_mode = True