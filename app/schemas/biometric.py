from pydantic import BaseModel, field_validator, ConfigDict
from datetime import datetime
from typing import Optional, List
from app.schemas.base import PaginatedResponse


class BiometricBase(BaseModel):
    biometric_type: str
    timestamp: datetime
    unit: Optional[str] = None


class BiometricIn(BiometricBase):
    value: Optional[float] = None
    systolic: Optional[int] = None
    diastolic: Optional[int] = None

    @field_validator("biometric_type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        valid_types = ["glucose", "weight", "blood_pressure"]
        if v.lower() not in valid_types:
            raise ValueError(f"Type must be one of {valid_types}")
        return v.lower()

    @field_validator("value")
    @classmethod
    def validate_value(cls, v: Optional[float], info) -> Optional[float]:
        if info.data.get("biometric_type") != "blood_pressure" and v is None:
            raise ValueError("Value is required for non-blood-pressure metrics")
        return v

    @field_validator("systolic", "diastolic")
    @classmethod
    def validate_bp(cls, v: Optional[int], info) -> Optional[int]:
        field_name = info.field_name
        if info.data.get("biometric_type") == "blood_pressure" and v is None:
            raise ValueError(f"{field_name} is required for blood pressure")
        return v


class BiometricOut(BiometricIn):
    id: int
    patient_id: int

    model_config = ConfigDict(
        from_attributes=True,  # Replaces orm_mode
        json_encoders={datetime: lambda v: v.isoformat()},
    )


class BiometricUpsert(BaseModel):
    biometric_type: str
    timestamp: datetime
    unit: Optional[str] = None
    value: Optional[float] = None
    systolic: Optional[int] = None
    diastolic: Optional[int] = None

    @field_validator("biometric_type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        valid_types = ["glucose", "weight", "blood_pressure"]
        if v.lower() not in valid_types:
            raise ValueError(f"Type must be one of {valid_types}")
        return v.lower()

    model_config = ConfigDict(from_attributes=True)


class BiometricPaginated(PaginatedResponse):
    data: List[BiometricOut]


class AnalyticsOut(BaseModel):
    id: int
    patient_id: int
    biometric_type: str
    hour_start: datetime
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    avg_value: Optional[float] = None
    count: int

    model_config = ConfigDict(
        from_attributes=True, json_encoders={datetime: lambda v: v.isoformat()}
    )


class AnalyticsPaginated(PaginatedResponse):
    data: List[AnalyticsOut]
