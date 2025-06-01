from pydantic import BaseModel, validator
from datetime import datetime
from app.schemas.base import PaginatedResponse
from typing import List, Optional


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

class BiometricPaginated(PaginatedResponse):
    data: List[BiometricOut]